/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.caffeine.internal

import akka.stream.alpakka.join.Store
import akka.stream.stage.AsyncCallback
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.concurrent.duration.FiniteDuration

class CaffeineStore[K, R](expired: AsyncCallback[R], ttl: FiniteDuration, scaffeine: Scaffeine[Any, Any] = Scaffeine())
    extends Store[K, R] {

  private val cache: Cache[K, R] = scaffeine
    .expireAfterWrite(ttl)
    .removalListener[K, R] {
      case (_, element, cause) =>
        cause match {
          case RemovalCause.EXPIRED =>
            expired.invoke(element)
          case _ =>
        }
    }
    .build[K, R]()

  def getIfPresent(key: K): Option[R] = cache.getIfPresent(key)

  def invalidate(key: K): Unit = cache.invalidate(key)

  def cleanUp(): Unit = cache.cleanUp()

  def put(key: K, r: R): Unit = cache.put(key, r)

  def estimatedSize(): Long = cache.estimatedSize()
}
