/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import javax.jms.Message

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSourceSettings, JmsSourceStage}

import scala.collection.JavaConversions

object JmsSource {

  /**
   * Java API: Creates an [[JmsSource]]
   */
  def create(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[Message, NotUsed] =
    akka.stream.javadsl.Source.fromGraph(new JmsSourceStage(jmsSourceSettings))

  /**
   * Java API: Creates an [[JmsSource]]
   */
  def textSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[String, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.textSource(jmsSourceSettings).asJava

  /**
   * Java API: Creates an [[JmsSource]] for byte arrays
   */
  def bytesSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[Array[Byte], NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.bytesSource(jmsSourceSettings).asJava

  /**
   * Java API: Creates an [[JmsSource]] for Maps with primitive data types
   */
  def mapSource(
      jmsSourceSettings: JmsSourceSettings
  ): akka.stream.javadsl.Source[java.util.Map[String, Any], NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource
      .mapSource(jmsSourceSettings)
      .map(scalaMap => JavaConversions.mapAsJavaMap(scalaMap))
      .asJava

  /**
   * Java API: Creates an [[JmsSource]] for serializable objects
   */
  def objectSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[java.io.Serializable, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.objectSource(jmsSourceSettings).asJava
  /**
   * Java API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   * @param jmsSettings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(jmsSettings: JmsSourceSettings): akka.stream.javadsl.Source[AckEnvelope, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsAckSourceStage(jmsSettings))

  /**
   * Java API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   * @param jmsSettings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(jmsSettings: JmsSourceSettings): akka.stream.javadsl.Source[TxEnvelope, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsTxSourceStage(jmsSettings))
}
