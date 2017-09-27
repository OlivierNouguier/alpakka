/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.internal

import akka.stream.alpakka.join.{Aggregator, Store}
import akka.stream.stage._
import akka.stream.{FanOutShape2, _}

import scala.language.postfixOps

class AggregatorFanOut2Stage[K, V, R](store: AsyncCallback[R] => Store[K, R], aggregator: Aggregator[K, V, R])
    extends GraphStage[FanOutShape2[V, R, R]] {

  private val in = Inlet[V]("in")
  private val out = Outlet[(R)]("out")
  private val exp = Outlet[(R)]("exp")

  override val shape = new FanOutShape2(in, out, exp)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new AggregatorGraphLogic[K, V, R](store, shape, aggregator, in) with InHandler {

      override protected def pumpComplete(): Boolean =
        isAvailable(out) && {
          if (completeQueue.isEmpty) {
            if (!isClosed(in) && !hasBeenPulled(in))
              pull(in)
            else checkComplete(out)
            false
          } else {
            val e = completeQueue.dequeue()
            push(out, e)
            true
          }
        }

      override protected def pumpExp(): Boolean =
        isAvailable(exp) && {
          if (expiredQueue.isEmpty) {
            if (!isClosed(in) && !hasBeenPulled(in))
              pull(in)
            else checkComplete(exp)
            false
          } else {
            val e = expiredQueue.dequeue()
            push(exp, e)
            true
          }
        }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          pumpComplete()
      })
      setHandler(exp, new OutHandler {
        override def onPull(): Unit =
          pumpExp()
      })

    }
}
