import com.orientechnologies.orient.client.remote.OServerAdmin
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ExecutionContext, Future}

package object util {

  val conf = ConfigFactory.load()

  val (dbHost, dbPort, dbName, dbRoot, dbPassword, dbMaxPartitionSize, dbMaxPool) = (
    conf.getString("db.host"),
    conf.getString("db.port"),
    conf.getString("db.name"),
    conf.getString("db.root"),
    conf.getString("db.password"),
    conf.getInt("db.max-partition-size"),
    conf.getInt("db.max-pool")
  )

  val dbUrl = s"remote:$dbHost:$dbPort/$dbName"

  val serverAdmin = new OServerAdmin(dbUrl).connect(dbRoot, dbPassword)


  /**
    * Evaluates block and measures how much time in ms did it take to finish
    * @param block Function to evaluate
    * @return Pair of result and time taken to accomplish it in ms
    */
  def time[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  /**
    * Measures how much time did it take for future to finish starting from the moment this method was called
    * @param future Future to be evaluated
    * @return Future of pair of result of computation and time taken to accomplish it in ms
    */
  def timeFuture[R](future: Future[R])(implicit ec: ExecutionContext): Future[(R, Long)] = {
    val t0 = System.currentTimeMillis()
    future.map { result =>
      val t1 = System.currentTimeMillis()
      (result, t1 - t0)
    }
  }


}
