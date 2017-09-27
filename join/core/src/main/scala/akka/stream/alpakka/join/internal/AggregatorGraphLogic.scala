/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.internal

import akka.stream.alpakka.join.{Aggregator, Store}
import akka.stream.stage.{AsyncCallback, GraphStageLogic, InHandler}
import akka.stream.{Inlet, Outlet, Shape}

import scala.annotation.tailrec
import scala.collection.mutable

abstract class AggregatorGraphLogic[K, V, R](storeA: AsyncCallback[R] => Store[K, R],
                                             shape: Shape,
                                             aggregator: Aggregator[K, V, R],
                                             in: Inlet[V])
    extends GraphStageLogic(shape)
    with InHandler {

  protected var store: Store[K, R] = _

  protected val completeQueue = mutable.Queue.empty[R]

  protected val expiredQueue = mutable.Queue.empty[R]

  var closing = false

  override def preStart(): Unit = {

    val expired = getAsyncCallback[R]({ r =>
      if (onAir.remove(aggregator.outKey(r)))
        expiredQueue.enqueue(r)
      pump()
    })

    store = storeA(expired)

  }

  /**
   * Hold the keys currently in cache.
   * Needed to prune reconciled and expired aggregate.
   */
  protected val onAir = mutable.Set[K]()

  protected def pumpExp(): Boolean

  protected def pumpComplete(): Boolean

  @tailrec final def pump(): Unit = {
    val a = pumpComplete()
    val b = pumpExp()
    if (a || b) pump()
  }

  protected def checkComplete(outlet: Outlet[_]): Unit =
    if (closing && onAir.isEmpty)
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
      onAir.remove(key)
      completeQueue.enqueue(r)
      store.cleanUp()

    } else {
      if (old.isEmpty)
        onAir.add(key)
      store.put(key, r)
    }
    pump()
  }

  override def onUpstreamFinish(): Unit = {

    while (store.estimatedSize() > 0) {
      store.cleanUp()
    }
    closing = true
    pump()
  }
}
