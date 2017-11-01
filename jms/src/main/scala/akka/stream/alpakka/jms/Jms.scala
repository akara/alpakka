/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.time.Duration
import java.util
import javax.jms.{ConnectionFactory, Message}

import scala.collection.JavaConversions._

case class AckEnvelope private[jms] (message: Message, private val jmsSession: JmsAckSession) {

  @volatile var committed = false

  def acknowledge(): Unit = if (!committed) jmsSession.ack(message)
}

case class TxEnvelope private[jms] (message: Message, private val jmsSession: JmsTxSession) {

  @volatile var committed = false

  def commit(): Unit = if (!committed) jmsSession.commit()

  def rollback(): Unit = if (!committed) jmsSession.rollback()
}

sealed trait JmsSettings {
  def connectionFactory: ConnectionFactory
  def destination: Option[Destination]
  def credentials: Option[Credentials]
}

sealed trait Destination
final case class Topic(name: String) extends Destination
final case class Queue(name: String) extends Destination

sealed trait JmsHeader
final case class JmsCorrelationId(jmsCorrelationId: String) extends JmsHeader
final case class JmsReplyTo(jmsDestination: Destination) extends JmsHeader
final case class JmsType(jmsType: String) extends JmsHeader

object JmsCorrelationId {

  /**
   * Java API: create  [[JmsCorrelationId]]
   */
  def create(correlationId: String) = JmsCorrelationId(correlationId)
}

object JmsReplyTo {

  /**
   * Java API: create  [[JmsCorrelationId]]
   */
  def queue(name: String) = JmsReplyTo(Queue(name))

  /**
   * Java API: create  [[JmsCorrelationId]]
   */
  def topic(name: String) = JmsReplyTo(Topic(name))
}

object JmsType {

  /**
   * Java API: create  [[JmsCorrelationId]]
   */
  def create(jmsType: String) = JmsType(jmsType)
}

final case class JmsTextMessage(body: String,
                                headers: Set[JmsHeader] = Set.empty,
                                properties: Map[String, Any] = Map.empty) {

  /**
   * Java API: defines JMSType [[JmsTextMessage]]
   */
  def withHeader(jmsHeader: JmsHeader) = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty [[JmsTextMessage]]
   */
  def withProperty(name: String, value: Any) = copy(properties = properties + (name -> value))

  /**
   * Java API: add [[JmsTextMessage]]
   */
  @deprecated("Unclear method name, use withProperty instead")
  def add(name: String, value: Any) = withProperty(name, value)
}

object JmsTextMessage {

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String) = JmsTextMessage(body = body, headers = Set.empty, properties = Map.empty)

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String, headers: util.Set[JmsHeader]) =
    JmsTextMessage(body = body, headers = headers.toSet, properties = Map.empty)

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String, properties: util.Map[String, Any]) =
    JmsTextMessage(body = body, headers = Set.empty, properties = properties.toMap)

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String, headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsTextMessage(body = body, headers = headers.toSet, properties = properties.toMap)
}

object JmsSourceSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSourceSettings(connectionFactory)

}

sealed trait AbstractJmsSourceSettings extends JmsSettings {
  def connectionFactory: ConnectionFactory
  def destination: Option[Destination]
  def credentials: Option[Credentials]
  def sessionCount: Int
  def selector: Option[String]
}

final case class JmsSourceSettings(connectionFactory: ConnectionFactory,
                                   destination: Option[Destination] = None,
                                   credentials: Option[Credentials] = None,
                                   sessionCount: Int = 1,
                                   bufferSize: Int = 100,
                                   selector: Option[String] = None)
    extends AbstractJmsSourceSettings {
  def withCredential(credentials: Credentials): JmsSourceSettings = copy(credentials = Some(credentials))
  def withSessionCount(count: Int): JmsSourceSettings = copy(sessionCount = count)
  def withBufferSize(size: Int): JmsSourceSettings = copy(bufferSize = size)
  def withQueue(name: String): JmsSourceSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsSourceSettings = copy(destination = Some(Topic(name)))
  def withSelector(selector: String): JmsSourceSettings = copy(selector = Some(selector))
}

object JmsSinkSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSinkSettings(connectionFactory)

}

final case class JmsSinkSettings(connectionFactory: ConnectionFactory,
                                 destination: Option[Destination] = None,
                                 credentials: Option[Credentials] = None,
                                 timeToLive: Option[Duration] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsSinkSettings = copy(credentials = Some(credentials))
  def withQueue(name: String): JmsSinkSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsSinkSettings = copy(destination = Some(Topic(name)))
  def withTimeToLive(ttl: Duration): JmsSinkSettings = copy(timeToLive = Some(ttl))
}

final case class Credentials(username: String, password: String)
