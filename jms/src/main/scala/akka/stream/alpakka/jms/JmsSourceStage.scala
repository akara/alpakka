/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import javax.jms._

import akka.stream.stage._
import akka.stream._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

final class JmsSourceStage(settings: JmsSourceSettings)
    extends GraphStageWithMaterializedValue[SourceShape[Message], KillSwitch] {

  private val out = Outlet[Message]("JmsSource.out")

  override def shape: SourceShape[Message] = SourceShape[Message](out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, KillSwitch) = {
    val logic = new PreStreamAckLogic(shape, out, settings, inheritedAttributes)
    (logic, logic.killSwitch)
  }
}

final class JmsAckSourceStage(settings: JmsSourceSettings)
    extends GraphStageWithMaterializedValue[SourceShape[AckEnvelope], KillSwitch] {

  private val out = Outlet[AckEnvelope]("JmsSource.out")

  override def shape: SourceShape[AckEnvelope] = SourceShape[AckEnvelope](out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, KillSwitch) = {
    val logic = new ExplicitAckLogic(shape, out, settings, inheritedAttributes)
    (logic, logic.killSwitch)
  }
}

final class JmsTxSourceStage(settings: JmsSourceSettings)
    extends GraphStageWithMaterializedValue[SourceShape[TxEnvelope], KillSwitch] {

  private val out = Outlet[TxEnvelope]("JmsSource.out")

  override def shape: SourceShape[TxEnvelope] = SourceShape[TxEnvelope](out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, KillSwitch) = {
    val logic = new TxLogic(shape, out, settings, inheritedAttributes)
    (logic, logic.killSwitch)
  }
}

abstract class SourceStageLogic[T](shape: SourceShape[T],
                                   out: Outlet[T],
                                   settings: JmsSourceSettings,
                                   attributes: Attributes)
    extends GraphStageLogic(shape)
    with JmsConnector
    with StageLogging {

  override private[jms] def jmsSettings = settings
  private val queue = mutable.Queue[T]()
  private val stopping = new AtomicBoolean(false)
  private var stopped = false

  private val markStopped = getAsyncCallback[Unit] { _ =>
    stopped = true
    if (queue.isEmpty) completeStage()
  }

  private val markAborted = getAsyncCallback[Throwable] { ex =>
    stopped = true
    failStage(ex)
  }

  private[jms] def getDispatcher =
    attributes.get[ActorAttributes.Dispatcher](
      ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
    ) match {
      case ActorAttributes.Dispatcher("") =>
        ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
      case d => d
    }

  private[jms] val handleError = getAsyncCallback[Throwable] { e =>
    fail(out, e)
  }

  override def preStart(): Unit = initSessionAsync(getDispatcher)

  private[jms] val handleMessage = getAsyncCallback[T] { msg =>
    if (isAvailable(out)) {
      if (queue.isEmpty) {
        pushMessage(msg)
      } else {
        pushMessage(queue.dequeue())
        queue.enqueue(msg)
      }
    } else {
      queue.enqueue(msg)
    }
  }

  private[jms] def pushMessage(msg: T): Unit

  setHandler(out, new OutHandler {
    override def onPull(): Unit = {
      if (queue.nonEmpty) pushMessage(queue.dequeue())
      if (stopped && queue.isEmpty) completeStage()
    }
  })

  private def stopSessions(): Unit =
    if (stopping.compareAndSet(false, true))
      Future {
        try {
          jmsConnection.stop()
        } catch {
          case NonFatal(e) => log.error(e, "Error stopping JMS connection {}", jmsConnection)
        }
      } andThen {
        case _ =>
          val closeSessionFutures = jmsSessions.map { s =>
            val f = s.closeSessionAsync()
            f.onFailure { case e => log.error(e, "Error closing jms session") }
            f
          }
          Future.sequence(closeSessionFutures).onComplete { _ =>
            try {
              jmsConnection.close()
              log.info("JMS connection {} closed", jmsConnection)

              // By this time, after stopping connection, closing sessions, and closing connections, all async message
              // submissions to this stage should have been invoked. We invoke markStopped as the last item so it gets
              // delivered after all JMS messages are delivered. This will allow the stage to complete after all
              // pending messages are delivered, thus preventing message loss due to premature stage completion.
              markStopped.invoke(_)
            } catch {
              case NonFatal(e) => log.error(e, "Error closing JMS connection {}", jmsConnection)
            }
          }
      }

  private def abortSessions(ex: Throwable): Unit =
    if (stopping.compareAndSet(false, true)) {
      val stopConnectionFuture = Future {
        try {
          jmsConnection.stop()
        } catch {
          case NonFatal(e) => log.error(e, "Error stopping JMS connection {}", jmsConnection)
        }
      }
      val closeSessionFutures = jmsSessions.map { s =>
        val f = s.abortSessionAsync()
        f.onFailure { case e => log.error(e, "Error closing jms session") }
        f
      }
      Future.sequence(stopConnectionFuture +: closeSessionFutures).onComplete { _ =>
        try {
          jmsConnection.close()
          log.info("JMS connection {} closed", jmsConnection)
          markAborted.invoke(ex)
        } catch {
          case NonFatal(e) => log.error(e, "Error closing JMS connection {}", jmsConnection)
        }
      }
    }

  private[jms] def killSwitch = new KillSwitch {
    override def shutdown(): Unit = stopSessions()
    override def abort(ex: Throwable): Unit = abortSessions(ex)
  }

  override def postStop(): Unit = {
    queue.clear()
    stopSessions()
  }
}

class PreStreamAckLogic(shape: SourceShape[Message],
                        out: Outlet[Message],
                        settings: JmsSourceSettings,
                        attributes: Attributes)
    extends SourceStageLogic[Message](shape, out, settings, attributes) {

  private val bufferSize = (settings.bufferSize + 1) * settings.sessionCount

  private val backpressure = new Semaphore(bufferSize)

  private[jms] def createSession(connection: Connection, createDestination: Session => javax.jms.Destination) = {
    val session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    new JmsSession(connection, session, createDestination(session))
  }

  private[jms] def pushMessage(msg: Message): Unit = {
    push(out, msg)
    backpressure.release()
  }

  override private[jms] def onSessionOpened(jmsSession: JmsSession): Unit =
    jmsSession
      .createConsumer(settings.selector)
      .onComplete {
        case Success(consumer) =>
          consumer.setMessageListener(new MessageListener {
            def onMessage(message: Message): Unit = {
              backpressure.acquire()
              try {
                message.acknowledge()
                handleMessage.invoke(message)
              } catch {
                case e: JMSException =>
                  backpressure.release()
                  handleError.invoke(e)
              }
            }
          })
        case Failure(e) =>
          fail.invoke(e)
      }
}

class ExplicitAckLogic(shape: SourceShape[AckEnvelope],
                       out: Outlet[AckEnvelope],
                       settings: JmsSourceSettings,
                       attributes: Attributes)
    extends SourceStageLogic[AckEnvelope](shape, out, settings, attributes) {

  private val maxPendingAck = settings.bufferSize

  private[jms] def createSession(connection: Connection, createDestination: Session => javax.jms.Destination) = {
    val session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    new JmsAckSession(connection, session, createDestination(session), settings.bufferSize)
  }

  private[jms] def pushMessage(msg: AckEnvelope): Unit = push(out, msg)

  override private[jms] def onSessionOpened(jmsSession: JmsSession): Unit =
    jmsSession match {
      case session: JmsAckSession =>
        session.createConsumer(settings.selector).onComplete {
          case Success(consumer) =>
            consumer.setMessageListener(new MessageListener {

              var listenerStopped = false

              def onMessage(message: Message): Unit = {

                @tailrec
                def ackQueued(): Unit =
                  Option(session.ackQueue.poll()) match {
                    case Some(action) =>
                      try {
                        action()
                        session.pendingAck -= 1
                      } catch {
                        case e: java.lang.IllegalStateException if e.getMessage == "Shutdown" =>
                          listenerStopped = true
                      }
                      if (!listenerStopped) ackQueued()
                    case None =>
                  }

                if (!listenerStopped)
                  try {
                    val threadName = Thread.currentThread().getName
                    println(threadName + " Got message " + message.asInstanceOf[TextMessage].getText)
                    handleMessage.invoke(AckEnvelope(message, session))
                    session.pendingAck += 1
                    if (session.pendingAck > maxPendingAck) {
                      val action = session.ackQueue.take()
                      action()
                      session.pendingAck -= 1
                    }
                    ackQueued()
                  } catch {
                    case e: java.lang.IllegalStateException if e.getMessage == "Shutdown" =>
                      listenerStopped = true
                    case e: JMSException =>
                      handleError.invoke(e)
                  }
              }
            })
          case Failure(e) =>
            fail.invoke(e)
        }

      case _ =>
        throw new IllegalArgumentException(
          "Session must be of type JMSAckSession, it is a " +
          jmsSession.getClass.getName
        )
    }
}

class TxLogic(shape: SourceShape[TxEnvelope],
              out: Outlet[TxEnvelope],
              settings: JmsSourceSettings,
              attributes: Attributes)
    extends SourceStageLogic[TxEnvelope](shape, out, settings, attributes) {

  private[jms] def createSession(connection: Connection, createDestination: Session => javax.jms.Destination) = {
    val session = connection.createSession(true, Session.SESSION_TRANSACTED)
    new JmsTxSession(connection, session, createDestination(session))
  }

  private[jms] def pushMessage(msg: TxEnvelope): Unit = push(out, msg)

  override private[jms] def onSessionOpened(jmsSession: JmsSession): Unit =
    jmsSession match {
      case session: JmsTxSession =>
        session.createConsumer(settings.selector).onComplete {
          case Success(consumer) =>
            consumer.setMessageListener(new MessageListener {
              def onMessage(message: Message): Unit =
                try {
                  handleMessage.invoke(TxEnvelope(message, session))
                  val action = session.commitQueue.take()
                  action()
                } catch {
                  case e: JMSException =>
                    handleError.invoke(e)
                }
            })
          case Failure(e) =>
            fail.invoke(e)
        }

      case _ =>
        throw new IllegalArgumentException(
          "Session must be of type JMSAckSession, it is a " +
          jmsSession.getClass.getName
        )
    }
}
