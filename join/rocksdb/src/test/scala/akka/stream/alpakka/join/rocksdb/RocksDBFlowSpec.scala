/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.rocksdb

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.join.Aggregator
import akka.stream.alpakka.join.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import io.circe.{Decoder, Encoder}
import org.rocksdb.Options
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps

//#events
sealed trait Event {
  def id: String
}

case class EventA(id: String, data: String) extends Event

case class EventB(id: String, data: String) extends Event

case class EventC(id: String, data: String) extends Event

object CirceSerder {
  def serialise[A](value: A)(implicit enc: Encoder[A]): Array[Byte] =
    enc(value).noSpaces.getBytes("UTF-8")

  def deserialise[A](bytes: Array[Byte])(implicit dec: Decoder[A]): A =
    io.circe.jawn.parseByteBuffer(ByteBuffer.wrap(bytes)).flatMap(o => dec.decodeJson(o)).toOption.get
}

object JavaSerder {
  def serialise[A](value: A): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  def deserialise[A](bytes: Array[Byte]): A = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value.asInstanceOf[A]
  }
}

//#events

//#aggregated-event
case class AggregatedEvents(key: String, a: Option[EventA] = None, b: Option[EventB] = None, c: Option[EventC] = None) {
  def isComplete: Boolean = a.isDefined && b.isDefined && c.isDefined
}

//#aggregated-event

class RocksDBTestSuite extends WordSpec with Matchers {

  val options = new Options()
    .setCreateIfMissing(true)
    .setAllowConcurrentMemtableWrite(true)
    .setAllowMmapWrites(true)
    .setAllowFAllocate(true)

  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()

  //#aggregator
  val aggregator = new Aggregator[String, Event, AggregatedEvents] {
    override def newAggregate(key: String): AggregatedEvents = AggregatedEvents(key)

    override def inKey(v: Event): String = v.id

    override def outKey(r: AggregatedEvents): String = r.key

    override def combine(r: AggregatedEvents, v: Event): AggregatedEvents = v match {
      case b: EventA => r.copy(a = Some(b))
      case w: EventB => r.copy(b = Some(w))
      case c: EventC => r.copy(c = Some(c))
    }

    override def isComplete(r: AggregatedEvents): Boolean =
      r.isComplete

  }

  //#aggregator
  def source(to: Int) = Source(1 to to).mapConcat { id =>
    if (id % 3 != 0)
      List(EventA(s"id_$id", "a a"), EventB(s"id_$id", "a b")) //Uncomplete events
    else
      List(EventA(s"id_$id", "a a"), EventB(s"id_$id", "a b"), EventC(s"id_$id", "a c")) // Complete events
  }

  val rev = "\b" * 11

  "RoksDBFlowSage" should {
    "aggregate events in flow" in {

      var compConnt = 0L

      import io.circe.generic.auto._

      val to = 1000

      val f = source(to)
        .via(
          AggregatorFlow(
            aggregator,
            new RocksDBStore[AggregatedEvents](options, "/tmp/akkarocks.db")(CirceSerder.deserialise[AggregatedEvents],
                                                                             CirceSerder.serialise[AggregatedEvents])
          )
        )
        .runWith(Sink.foreach[AggregatedEvents] { r =>
          compConnt = compConnt + 1

          print(f"$rev$compConnt%10d")
        })

      Await.result(f, Duration.Inf)

      compConnt shouldEqual (to / 3)

    }

  }

}
