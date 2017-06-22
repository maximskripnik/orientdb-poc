import gremlin.scala._
import util._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class LoadTest(val connection: Connection, val n: Int) {
  import connection.graph

  def syncWrite(): Long = {
    println("Sync write execute...")

    time {
      (1 to n).foreach { _ =>
        graph + "syncLabel"
      }
    }._2
  }

  def syncRead(): Long = {
    println("Sync read execute...")

    time {
      (1 to n).foreach { _ =>
        graph.V
      }
    }._2
  }

  def syncUpdate(): Long = {
    println("Sync update execute...")

    val v1 = graph + "syncLabel"
    val v2 = graph + "syncLabel"
    val key = Key[String]("syncProperty")
    time {
      (1 to n).foreach { i =>
        if (i % 2 == 0)
          v1.setProperty(key, i.toString)
        else
          v2.setProperty(key, i.toString)
      }
    }._2
  }

  def asyncWrite(): Future[Long] = {
    val writeF = Future.sequence {
      (1 to n).map { _ =>
        Future {
          graph + "asyncLabel"
        }
      }
    }

    val resultF = timeFuture(writeF).map(_._2)

    while (!writeF.isCompleted) {
      println("Async write execute...")
      Thread.sleep(5000)
    }

    resultF
  }

  def asyncRead(): Future[Long] = {
    val readF = Future.sequence {
      (1 to n).map { _ =>
        Future {
          graph.V
        }
      }
    }

    val resultF = timeFuture(readF).map(_._2)

    while (!readF.isCompleted) {
      println("Async read execute...")
      Thread.sleep(5000)
    }

    resultF
  }

  def asyncUpdate(): Future[Long] = {
    val v1 = graph + "asyncLabel"
    val v2 = graph + "asyncLabel"
    val key = Key[String]("asyncProperty")

    val updateF = Future.sequence {
      (1 to n).map { i =>
        if (i % 2 == 0)
          Future { v1.setProperty(key, i.toString) }
        else
          Future { v2.setProperty(key, i.toString) }
      }
    }

    val resultF = timeFuture(updateF).map(_._2)

    while (!updateF.isCompleted) {
      println("Async update execute...")
      Thread.sleep(5000)
    }

    resultF
  }

}