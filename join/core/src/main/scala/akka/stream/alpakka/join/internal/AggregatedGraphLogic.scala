/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.internal

import akka.stream.alpakka.join.{Aggregator, Store}
import akka.stream.stage.{AsyncCallback, GraphStageLogic, InHandler}
import akka.stream.{Inlet, Outlet, Shape}

import scala.annotation.tailrec
import scala.collection.mutable

abstract class AggregatedGraphLogic[K, V, R](store: Store[K, R],
                                             shape: Shape,
                                             aggregator: Aggregator[K, V, R],
                                             in: Inlet[V])
    extends GraphStageLogic(shape)
    with InHandler {

  protected val completeQueue = mutable.Queue.empty[R]

  var closing = false

  /**
   * Hold the keys currently in cache.
   * Needed to prune reconciled and expired aggregate.
   */
  protected def pumpComplete(): Boolean

  @tailrec final def pump(): Unit =
    if (pumpComplete())
      pump()

  protected def checkComplete(outlet: Outlet[_]): Unit =
    if (closing && completeQueue.isEmpty)
      complete(outlet)

  setHandler(in, this)

  override def onPush(): Unit = {
    val a = grab(in)
    val key = aggregator.inKey(a)

    val old = store.getIfPresent(key)

    val r = aggregator.combine(old.getOrElse(aggregator.newAggregate(key)), a)

    if (aggregator.isComplete(r)) {
      if (old.isDefined)
        store.invalidate(key)
      completeQueue.enqueue(r)
      store.cleanUp()

    } else {
      store.put(key, r)
    }
    pump()
  }

  override def onUpstreamFinish(): Unit = {

    closing = true
    pump()
  }
}
