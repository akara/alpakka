/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms
import javax.jms.{Connection, MessageProducer, Session, TextMessage}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, StageLogging}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}

final class JmsSinkStage(settings: JmsSinkSettings) extends GraphStage[SinkShape[JmsTextMessage]] {

  private val in = Inlet[JmsTextMessage]("JmsSink.in")

  def shape: SinkShape[JmsTextMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes =
    ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector with StageLogging {

      private var jmsProducer: MessageProducer = _

      private[jms] def jmsSettings = settings

      private[jms] def createSession(connection: Connection, createDestination: Session => jms.Destination) = {
        val session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
        new JmsSession(connection, session, createDestination(session))
      }

      override def preStart(): Unit = {

        jmsSessions = openSessions()
        // TODO: Remove hack to limit publisher to single session.
        val jmsSession = jmsSessions.head
        jmsProducer = jmsSession.session.createProducer(jmsSession.destination)
        if (settings.timeToLive.nonEmpty) {
          jmsProducer.setTimeToLive(settings.timeToLive.get.toMillis)
        }
        pull(in)
      }

      setHandler(
        in,
        new InHandler {

          def onPush(): Unit = {

            def createDestination(destination: Destination): _root_.javax.jms.Destination =
              destination match {
                case Queue(name) => jmsSessions.head.session.createQueue(name)
                case Topic(name) => jmsSessions.head.session.createTopic(name)
              }

            val elem: JmsTextMessage = grab(in)
            val textMessage: TextMessage = jmsSessions.head.session.createTextMessage(elem.body)

            elem.headers.foreach {
              case JmsType(jmsType) => textMessage.setJMSType(jmsType)
              case JmsReplyTo(destination) => textMessage.setJMSReplyTo(createDestination(destination))
              case JmsCorrelationId(jmsCorrelationId) => textMessage.setJMSCorrelationID(jmsCorrelationId)
            }

            elem.properties.foreach {
              case (key, v) =>
                v match {
                  case v: String => textMessage.setStringProperty(key, v)
                  case v: Int => textMessage.setIntProperty(key, v)
                  case v: Boolean => textMessage.setBooleanProperty(key, v)
                  case v: Byte => textMessage.setByteProperty(key, v)
                  case v: Short => textMessage.setShortProperty(key, v)
                  case v: Long => textMessage.setLongProperty(key, v)
                  case v: Double => textMessage.setDoubleProperty(key, v)
                }
            }

            jmsProducer.send(textMessage)
            pull(in)
          }
        }
      )

      override def postStop(): Unit =
        jmsSessions.foreach(_.closeSession())
    }

}
