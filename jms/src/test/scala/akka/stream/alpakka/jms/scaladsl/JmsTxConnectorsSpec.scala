/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import javax.jms.{JMSException, TextMessage}

import akka.NotUsed
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{KillSwitch, ThrottleMode}
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.Inspectors._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._

class JmsTxConnectorsSpec extends JmsSpec {

  override implicit val patienceConfig = PatienceConfig(1.minute)

  "The JMS Transactional Connectors" should {
    "publish and consume strings through a queue" in withServer() { ctx =>
      val url: String = ctx.url
      //#connection-factory
      val connectionFactory = new ActiveMQConnectionFactory(url)
      //#connection-factory

      //#create-text-sink
      val jmsSink: Sink[String, NotUsed] = JmsSink.textSink(
        JmsSinkSettings(connectionFactory).withQueue("test")
      )
      //#create-text-sink

      //#run-text-sink
      val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)
      Source(in).runWith(jmsSink)
      //#run-text-sink

      //#create-tx-source
      val jmsSource: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(5).withQueue("test")
      )
      //#create-tx-source

      //#run-tx-source
      val result = jmsSource
        .take(in.size)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
        .runWith(Sink.seq)
      //#run-tx-source

      result.futureValue should contain theSameElementsAs in
    }

    "publish and consume JMS text messages with properties through a queue" in withServer() { ctx =>
      val url: String = ctx.url
      val connectionFactory = new ActiveMQConnectionFactory(url)

      //#create-jms-sink
      val jmsSink: Sink[JmsTextMessage, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withQueue("numbers")
      )
      //#create-jms-sink

      //#create-messages-with-properties
      val msgsIn = 1 to 100 map { n =>
        JmsTextMessage(n.toString).add("Number", n).add("IsOdd", n % 2 == 1).add("IsEven", n % 2 == 0)
      }
      //#create-messages-with-properties

      Source(msgsIn).runWith(jmsSink)

      //#create-jms-source
      val jmsSource: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(5).withQueue("numbers")
      )
      //#create-jms-source

      //#run-jms-source
      val result = jmsSource
        .take(msgsIn.size)
        .map { env =>
          env.commit(); env.message
        }
        .runWith(Sink.seq)
      //#run-jms-source

      // The sent message and the receiving one should have the same properties
      val sortedResult = result.futureValue.sortBy(msg => msg.getIntProperty("Number"))
      forAll(sortedResult.zip(msgsIn)) {
        case (out, in) =>
          out.getIntProperty("Number") shouldEqual in.properties("Number")
          out.getBooleanProperty("IsOdd") shouldEqual in.properties("IsOdd")
          out.getBooleanProperty("IsEven") shouldEqual in.properties("IsEven")
      }
    }

    "ensure re-delivery when rollback JMS text messages through a queue" in withServer() { ctx =>
      val url: String = ctx.url
      val connectionFactory = new ActiveMQConnectionFactory(url)

      val jmsSink: Sink[JmsTextMessage, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withQueue("numbers")
      )
      val msgsIn = 1 to 100 map { n =>
        JmsTextMessage(n.toString).add("Number", n)
      }

      Source(msgsIn).runWith(jmsSink)

      val jmsSource: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(5).withQueue("numbers")
      )

      val expectedElements = (1 to 100) ++ (2 to 100 by 2) map (_.toString)

      val rolledBackSet = ConcurrentHashMap.newKeySet[Int]()

      val result = jmsSource
        .take(expectedElements.size)
        .map { env =>
          val id = env.message.getIntProperty("Number")
          if (id % 2 == 0 && !rolledBackSet.contains(id)) {
            rolledBackSet.add(id)
            env.rollback()
          } else {
            env.commit()
          }
          env.message.asInstanceOf[TextMessage].getText
        }
        .runWith(Sink.seq)

      result.futureValue should contain theSameElementsAs expectedElements
    }

    "publish JMS text messages with properties through a queue and consume them with a selector" in withServer() {
      ctx =>
        val url: String = ctx.url
        val connectionFactory = new ActiveMQConnectionFactory(url)

        val jmsSink: Sink[JmsTextMessage, NotUsed] = JmsSink(
          JmsSinkSettings(connectionFactory).withQueue("numbers")
        )

        val msgsIn = 1 to 100 map { n =>
          JmsTextMessage(n.toString).add("Number", n).add("IsOdd", n % 2 == 1).add("IsEven", n % 2 == 0)
        }
        Source(msgsIn).runWith(jmsSink)

        //#create-jms-source-with-selector
        val jmsSource = JmsSource.txSource(
          JmsSourceSettings(connectionFactory).withSessionCount(5).withQueue("numbers").withSelector("IsOdd = TRUE")
        )
        //#create-jms-source-with-selector

        //#assert-only-odd-messages-received
        val oddMsgsIn = msgsIn.filter(msg => msg.body.toInt % 2 == 1)
        val result = jmsSource
          .take(oddMsgsIn.size)
          .map { env =>
            env.commit(); env.message
          }
          .runWith(Sink.seq)
        // We should have only received the odd numbers in the list

        val sortedResult = result.futureValue.sortBy(msg => msg.getIntProperty("Number"))
        forAll(sortedResult.zip(oddMsgsIn)) {
          case (out, in) =>
            out.getIntProperty("Number") shouldEqual in.properties("Number")
            out.getBooleanProperty("IsOdd") shouldEqual in.properties("IsOdd")
            out.getBooleanProperty("IsEven") shouldEqual in.properties("IsEven")
            // Make sure we are only receiving odd numbers
            out.getIntProperty("Number") % 2 shouldEqual 1
        }
      //#assert-only-odd-messages-received
    }

    "applying backpressure when the consumer is slower than the producer" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)
      Source(in).runWith(JmsSink.textSink(JmsSinkSettings(connectionFactory).withQueue("test")))

      val jmsSource: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(5).withQueue("test")
      )

      val result = jmsSource
        .throttle(10, 1.second, 1, ThrottleMode.shaping)
        .take(in.size)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
        .runWith(Sink.seq)

      result.futureValue should contain theSameElementsAs in
    }

    "disconnection should fail the stage" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val result = JmsSource.txSource(JmsSourceSettings(connectionFactory).withQueue("test")).runWith(Sink.seq)
      Thread.sleep(500)
      ctx.broker.stop()
      result.failed.futureValue shouldBe an[JMSException]
    }

    "publish and consume elements through a topic " in withServer() { ctx =>
      import system.dispatcher

      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      //#create-topic-sink
      val jmsTopicSink: Sink[String, NotUsed] = JmsSink.textSink(
        JmsSinkSettings(connectionFactory).withTopic("topic")
      )
      //#create-topic-sink
      val jmsTopicSink2: Sink[String, NotUsed] = JmsSink.textSink(
        JmsSinkSettings(connectionFactory).withTopic("topic")
      )

      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val inNumbers = (1 to 10).map(_.toString)

      //#create-topic-source
      val jmsTopicSource: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(1).withTopic("topic")
      )
      //#create-topic-source
      val jmsSource2: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(1).withTopic("topic")
      )

      val expectedSize = in.size + inNumbers.size
      //#run-topic-source
      val result1 = jmsTopicSource
        .take(expectedSize)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
        .runWith(Sink.seq)
        .map(_.sorted)
      val result2 = jmsSource2
        .take(expectedSize)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
        .runWith(Sink.seq)
        .map(_.sorted)
      //#run-topic-source

      //We wait a little to be sure that the source is connected
      Thread.sleep(500)

      //#run-topic-sink
      Source(in).runWith(jmsTopicSink)
      //#run-topic-sink
      Source(inNumbers).runWith(jmsTopicSink2)

      val expectedList: List[String] = in ++ inNumbers
      result1.futureValue should contain theSameElementsAs expectedList
      result2.futureValue should contain theSameElementsAs expectedList
    }

    "ensure no message loss when stopping a stream" in withServer() { ctx =>
      val url: String = ctx.url
      val connectionFactory = new ActiveMQConnectionFactory(url)

      val jmsSink: Sink[JmsTextMessage, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withQueue("numbers")
      )

      val numsIn = 1 to 100
      val msgsIn = numsIn map { n =>
        JmsTextMessage(n.toString).add("Number", n)
      }

      Source(msgsIn).runWith(jmsSink)

      val jmsSource: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(5).withQueue("numbers")
      )

      val resultQueue = new LinkedBlockingQueue[String]()

      val killSwitch = jmsSource
        .take(msgsIn.size)
        .throttle(10, 1.second, 2, ThrottleMode.shaping)
        .map { env =>
          resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
          env.commit()
        }
        .to(Sink.ignore)
        .run()

      Thread.sleep(2000)
      killSwitch.shutdown()

      Thread.sleep(500)
      println("Elements in resultQueue before first shutdown: " + resultQueue.size)

      val killSwitch2 = jmsSource
        .take(msgsIn.size - resultQueue.size)
        .map { env =>
          resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
          env.commit()
        }
        .to(Sink.ignore)
        .run()

      val resultList = new mutable.ArrayBuffer[String](numsIn.size)

      @tailrec
      def keepPolling(): Unit =
        Option(resultQueue.poll(2, TimeUnit.SECONDS)) match {
          case Some(entry) =>
            resultList += entry
            keepPolling()
          case None =>
        }

      keepPolling()

      println("Elements in resultList now: " + resultList.size)
      killSwitch2.shutdown()

      resultList should contain theSameElementsAs numsIn.map(_.toString)
    }

    "ensure no message loss when aborting a stream" in withServer() { ctx =>
      val url: String = ctx.url
      val connectionFactory = new ActiveMQConnectionFactory(url)

      val jmsSink: Sink[JmsTextMessage, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withQueue("numbers")
      )

      val numsIn = 1 to 100
      val msgsIn = numsIn map { n =>
        JmsTextMessage(n.toString).add("Number", n)
      }

      Source(msgsIn).runWith(jmsSink)

      val jmsSource: Source[TxEnvelope, KillSwitch] = JmsSource.txSource(
        JmsSourceSettings(connectionFactory).withSessionCount(5).withQueue("numbers")
      )

      val resultQueue = new LinkedBlockingQueue[String]()

      val killSwitch = jmsSource
        .take(msgsIn.size)
        .throttle(10, 1.second, 2, ThrottleMode.shaping)
        .map { env =>
          resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
          env.commit()
        }
        .to(Sink.ignore)
        .run()

      Thread.sleep(2000)
      killSwitch.abort(new Exception("Test exception"))

      Thread.sleep(500)
      println("Elements in resultQueue before aborting: " + resultQueue.size)

      val killSwitch2 = jmsSource
        .take(msgsIn.size - resultQueue.size)
        .map { env =>
          resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
          env.commit()
        }
        .to(Sink.ignore)
        .run()

      val resultList = new mutable.ArrayBuffer[String](numsIn.size)

      @tailrec
      def keepPolling(): Unit =
        Option(resultQueue.poll(2, TimeUnit.SECONDS)) match {
          case Some(entry) =>
            resultList += entry
            keepPolling()
          case None =>
        }

      keepPolling()

      println("Elements in resultList now: " + resultList.size)
      killSwitch2.shutdown()

      resultList should contain theSameElementsAs numsIn.map(_.toString)
    }
  }
}
