/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join.rocksdb

import akka.stream.alpakka.join.Store
import org.rocksdb.{Options, RocksDB}

object RocksDBStore {
  RocksDB.loadLibrary()
}

class RocksDBStore[R](options: Options, path: String)(fromByte: Array[Byte] => R, toBytes: R => Array[Byte])
    extends Store[String, R] {

  import scala.language.implicitConversions

  private implicit def t2b(str: String): Array[Byte] = str.getBytes("UTF8")

  val rockdDB = RocksDB.open(options, path)
  //TtlDB.open(options, path, 10, false)

  override def getIfPresent(key: String): Option[R] = Option(rockdDB.get(key.toString)).map(fromByte)

  override def invalidate(key: String): Unit = rockdDB.delete(key)

  override def cleanUp(): Unit = rockdDB.compactRange()

  override def put(key: String, r: R): Unit = rockdDB.put(key, toBytes(r))

  override def estimatedSize(): Long = -1
}
