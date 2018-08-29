/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.ActorAttributes.Dispatcher
import akka.stream.stage.GraphStageLogic
import akka.stream.{ActorMaterializer, ActorMaterializerHelper}
import javax.jms

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.control.NonFatal

/**
 * Internal API
 */
private[jms] trait JmsConnector { this: GraphStageLogic =>

  implicit private[jms] var ec: ExecutionContext = _

  private[jms] var jmsConnection: Option[jms.Connection] = None

  private[jms] var jmsSessions = Seq.empty[JmsSession]

  private[jms] def jmsSettings: JmsSettings

  private[jms] def onSessionOpened(jmsSession: JmsSession): Unit = {}

  private[jms] def fail = getAsyncCallback[Throwable](e => failStage(e))

  private def onConnection = getAsyncCallback[jms.Connection] { c =>
    jmsConnection = Some(c)
  }

  private def onSession =
    getAsyncCallback[JmsSession] { session =>
      jmsSessions :+= session
      onSessionOpened(session)
    }

  private def setExecutionContext(dispatcher: Dispatcher): Unit =
    ec = materializer match {
      case m: ActorMaterializer => m.system.dispatchers.lookup(dispatcher.dispatcher)
      case x => throw new IllegalArgumentException(s"Stage only works with the ActorMaterializer, was: $x")
    }

  private[jms] def initSessionAsync(dispatcher: Dispatcher): Future[_] = {
    setExecutionContext(dispatcher)
    val sessionsFuture = createSessions()
    sessionsFuture.foreach { sessions =>
      sessions.foreach { session =>
        onSession.invoke(session)
      }
    }
    sessionsFuture.onFailure {
      case e: Exception => fail.invoke(e)
    }
    sessionsFuture
  }

  private[jms] def createSession(connection: jms.Connection,
                                 createDestination: jms.Session => jms.Destination): JmsSession

  sealed trait ConnectionStatus
  case object Connecting extends ConnectionStatus
  case object Connected extends ConnectionStatus
  case object TimedOut extends ConnectionStatus

  private def startConnection()(implicit system: ActorSystem): Future[jms.Connection] = {
    val factory = jmsSettings.connectionFactory
    val connectionRef: AtomicReference[Option[jms.Connection]] = new AtomicReference(None)

    // status is also the decision point between the two futures below which one will win.
    val status = new AtomicReference[ConnectionStatus](Connecting)

    val connectionFuture = Future {
      val connection = jmsSettings.credentials match {
        case Some(Credentials(username, password)) => factory.createConnection(username, password)
        case _ => factory.createConnection()
      }
      if (status.get == Connecting) { // `cancelled` can be set at any point. So we have to check whether to continue.
        connectionRef.set(Some(connection))
        connection.start()
      }
      // ... and close if the connection is not to be used, don't return the connection
      if (!status.compareAndSet(Connecting, Connected)) {
        connectionRef.get.foreach(_.close())
        connectionRef.set(None)
        throw new TimeoutException("Received timed out signal trying to establish connection")
      } else connection
    }

    val timeoutFuture = after(jmsSettings.connectionRetrySettings.connectTimeout, system.scheduler) {
      // Even if the timer goes off, the connection may already be good. We use the
      // status field and an atomic compareAndSet to see whether we should indeed time out, or just return
      // the connection. In this case it does not matter which future returns. Both will have the right answer.
      if (status.compareAndSet(Connecting, TimedOut)) {
        connectionRef.get.foreach(_.close())
        connectionRef.set(None)
        Future.failed(new TimeoutException("Timed out trying to establish connection"))
      } else
        connectionRef.get match {
          case Some(connection) => Future.successful(connection)
          case None => Future.failed(new IllegalStateException("BUG: Connection reference not set when connected"))
        }
    }

    Future.firstCompletedOf(Iterator(connectionFuture, timeoutFuture))
  }

  private def startConnectionWithRetry(n: Int = 0, maxed: Boolean = false)
                                      (implicit system: ActorSystem): Future[jms.Connection] =
    startConnection().recoverWith {
      case e: jms.JMSSecurityException => Future.failed(e)
      case NonFatal(t) =>
        val retrySettings = jmsSettings.connectionRetrySettings
        import retrySettings._
        val nextN = n + 1
        if (maxRetries >= 0 && nextN > maxRetries) { // Negative maxRetries treated as infinite.
          Future.failed(ConnectionRetryException(s"Could not establish connection after $n retries.", t))
        } else {
          val delay =
            if (maxed) maxBackoff
            else waitTime(nextN)

          if (delay >= maxBackoff)
            after(maxBackoff, system.scheduler) {
              startConnectionWithRetry(nextN, maxed = true)
            }
          else
            after(delay, system.scheduler) {
              startConnectionWithRetry(nextN)
            }
        }
    }

  private[jms] def openSessions(dispatcher: Dispatcher): Future[Seq[JmsSession]] = {
    setExecutionContext(dispatcher)
    createSessions()
  }

  private def createSessions(): Future[Seq[JmsSession]] = {
    implicit val system: ActorSystem = ActorMaterializerHelper.downcast(materializer).system
    startConnectionWithRetry().map { connection =>
      connection.setExceptionListener(new jms.ExceptionListener {
        override def onException(exception: jms.JMSException) =
          fail.invoke(exception)
      })
      onConnection.invoke(connection)

      val createDestination = jmsSettings.destination match {
        case Some(destination) =>
          destination.create
        case _ => throw new IllegalArgumentException("Destination is missing")
      }

      val sessionCount = jmsSettings match {
        case settings: JmsConsumerSettings =>
          settings.sessionCount
        case _ => 1
      }

      0 until sessionCount map { _ =>
        createSession(connection, createDestination)
      }
    }
  }
}

private[jms] class JmsSession(val connection: jms.Connection,
                              val session: jms.Session,
                              val destination: jms.Destination) {

  private[jms] def closeSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { closeSession() }

  private[jms] def closeSession(): Unit = session.close()

  private[jms] def abortSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { abortSession() }

  private[jms] def abortSession(): Unit = closeSession()

  private[jms] def createProducer()(implicit ec: ExecutionContext): Future[jms.MessageProducer] =
    Future {
      session.createProducer(destination)
    }

  private[jms] def createConsumer(
      selector: Option[String]
  )(implicit ec: ExecutionContext): Future[jms.MessageConsumer] =
    Future {
      selector match {
        case None => session.createConsumer(destination)
        case Some(expr) => session.createConsumer(destination, expr)
      }
    }
}

private[jms] class JmsAckSession(override val connection: jms.Connection,
                                 override val session: jms.Session,
                                 override val destination: jms.Destination,
                                 val maxPendingAcks: Int)
    extends JmsSession(connection, session, destination) {

  private[jms] var pendingAck = 0
  private[jms] val ackQueue = new ArrayBlockingQueue[() => Unit](maxPendingAcks + 1)

  def ack(message: jms.Message): Unit = ackQueue.put(message.acknowledge _)

  override def closeSession(): Unit = stopMessageListenerAndCloseSession()

  override def abortSession(): Unit = stopMessageListenerAndCloseSession()

  private def stopMessageListenerAndCloseSession(): Unit = {
    ackQueue.put(() => throw StopMessageListenerException())
    session.close()
  }
}
