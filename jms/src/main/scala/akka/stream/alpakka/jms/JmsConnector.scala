/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.stream.ActorAttributes.Dispatcher
import akka.stream.ActorMaterializer
import akka.stream.stage.GraphStageLogic
import javax.jms

import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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

  private[jms] def initSessionAsync(dispatcher: Dispatcher): Future[Unit] = {
    setExecutionContext(dispatcher)
    val future = Future {
      val sessions = createSessions()
      sessions foreach { session =>
        onSession.invoke(session)
      }
    }
    future.onFailure {
      case e: Exception => fail.invoke(e)
    }
    future
  }

  private[jms] def createSession(connection: jms.Connection,
                                 createDestination: jms.Session => jms.Destination): JmsSession

  private def tryStartConnection(): Try[jms.Connection] = {
    val factory = jmsSettings.connectionFactory
    val connectionRef: AtomicReference[Option[jms.Connection]] = new AtomicReference(None)
    val cancelled = new AtomicBoolean(false)

    val connectionTry = Try {
      val connectionFuture = Future {
        val connection = jmsSettings.credentials match {
          case Some(Credentials(username, password)) => factory.createConnection(username, password)
          case _ => factory.createConnection()
        }
        if (!cancelled.get) { // `cancelled` can be set at any point. So we have to check whether to continue.
          connectionRef.set(Some(connection))
          connection.start()
        }
        if (cancelled.get) connection.close() // ... and clean up if the connection is not to be used.
        connection
      }
      Await.result(connectionFuture, jmsSettings.connectionRetrySettings.connectTimeout)
    }
    connectionTry.failed.foreach { _ =>
      cancelled.set(true) // Cancel the connection setup.
      Future { connectionRef.get().foreach(_.close()) }
    }
    connectionTry
  }

  @tailrec
  private def startConnectionWithRetry(n: Int = 0, maxed: Boolean = false): jms.Connection =
    tryStartConnection() match {
      case Success(connection) => connection
      case Failure(e: jms.JMSSecurityException) => // Login credentials failure, don't retry.
        fail.invoke(e)
        throw e
      case Failure(t) =>
        val retrySettings = jmsSettings.connectionRetrySettings
        import retrySettings._
        val nextN = n + 1
        if (maxRetries >= 0 && nextN > maxRetries) { // Negative maxRetries treated as infinite.
          fail.invoke(t)
          throw t
        }
        val delay = if (maxed) maxBackoff else initialRetry * Math.pow(nextN, backoffFactor)
        if (delay >= maxBackoff) {
          Thread.sleep(maxBackoff.toMillis)
          startConnectionWithRetry(nextN, maxed = true)
        } else {
          Thread.sleep(delay.toMillis)
          startConnectionWithRetry(nextN)
        }
    }

  private[jms] def openSessions(dispatcher: Dispatcher): Seq[JmsSession] = {
    setExecutionContext(dispatcher)
    createSessions()
  }

  private def createSessions(): Seq[JmsSession] = {
    val connection = startConnectionWithRetry()

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
