/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.internal

import akka.stream.alpakka.join.{Aggregator, Store}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.language.postfixOps

class AggregatedFlowStage[K, V, R](store: Store[K, R], aggregator: Aggregator[K, V, R])
    extends GraphStage[FlowShape[V, R]] {

  private val in = Inlet[V]("in")
  private val out = Outlet[R]("out")

  override val shape = new FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new AggregatedGraphLogic[K, V, R](store, shape, aggregator, in) with InHandler {

      override final def pumpComplete(): Boolean =
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

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          pump()
      })

    }
}
