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

}