/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import javax.jms.{Message, TextMessage}

import akka.stream.KillSwitch
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.Source

object JmsSource {

  /**
   * Scala API: Creates an [[JmsSource]] that auto-acknowledges.
   */
  def apply(jmsSettings: JmsSourceSettings): Source[Message, KillSwitch] =
    Source.fromGraph(new JmsSourceStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSource]] that auto-acknowledges.
   */
  def textSource(jmsSettings: JmsSourceSettings): Source[String, KillSwitch] =
    Source.fromGraph(new JmsSourceStage(jmsSettings)).map(msg => msg.asInstanceOf[TextMessage].getText)

  /**
   * Scala API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   * @param jmsSettings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(jmsSettings: JmsSourceSettings): Source[AckEnvelope, KillSwitch] =
    Source.fromGraph(new JmsAckSourceStage(jmsSettings))

  /**
   * Scala API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   * @param jmsSettings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(jmsSettings: JmsSourceSettings): Source[TxEnvelope, KillSwitch] =
    Source.fromGraph(new JmsTxSourceStage(jmsSettings))

}
