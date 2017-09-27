/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join

trait Store[K, R] {
  def getIfPresent(key: K): Option[R]

  def invalidate(key: K): Unit

  def cleanUp(): Unit

  def put(key: K, r: R): Unit

  def estimatedSize(): Long
}
