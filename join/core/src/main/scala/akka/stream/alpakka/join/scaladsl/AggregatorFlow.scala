/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.scaladsl

import akka.stream.alpakka.join.{Aggregator, Store}
import akka.stream.alpakka.join.internal.{AggregatedFlowStage, AggregatorFanOut2Stage, AggregatorFlowStage}
import akka.stream.stage.AsyncCallback

object AggregatorFlow {
  def apply[K, V, R](aggregator: Aggregator[K, V, R], store: Store[K, R]) =
    new AggregatedFlowStage(store, aggregator)
  def apply[K, V, R](aggregator: Aggregator[K, V, R])(store: AsyncCallback[R] => Store[K, R]) =
    new AggregatorFlowStage(store, aggregator)
  def tee[K, V, R](aggregator: Aggregator[K, V, R])(store: AsyncCallback[R] => Store[K, R]) =
    new AggregatorFanOut2Stage(store, aggregator)
}
