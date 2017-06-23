import gremlin.scala._
import util._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

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

    val vertices = (1 to n).map { _ =>
      graph + "syncLabel"
    }
    val key = Key[String]("syncProperty")

    time {
      vertices.zipWithIndex.foreach { case (v, i) =>
        graph.V(v.id).head.setProperty(key, s"$i value")
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
    val key = Key[String]("asyncProperty")
    val vertices = Await.result(
      Future.sequence {
        (1 to n).map { _ =>
          Future {
            graph + ("asyncLabel", key -> "initialValue")
          }
        }
      },
      Duration.Inf
    )

    val updateF = Future.sequence {
      vertices.zipWithIndex.map { case (v, i) =>
        Future {
          graph.V(v.id).head.setProperty(key, s"Updated $i value")
        }
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