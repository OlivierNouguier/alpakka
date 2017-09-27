/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.join

import org.rocksdb._

import scala.language.implicitConversions

object JoinTest extends App {

  implicit def t2b(str: String): Array[Byte] = str.getBytes("UTF8")

  val compactionOptionsFIFO = new CompactionOptionsFIFO()

  RocksDB.loadLibrary()
  val options = new Options()
    .setCreateIfMissing(true)

  val rocksDB = TtlDB.open(options, "/tmp/myrock.db", 10, false)
  rocksDB.put("test", "testval")

  private val builder = new java.lang.StringBuilder

  while (rocksDB.keyMayExist("test", builder)) {
    Thread.sleep(2000)
    //rocksDB.compactRange()
    println(builder.toString)
    builder.setLength(0)
  }

}
