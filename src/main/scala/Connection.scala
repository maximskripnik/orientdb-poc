import gremlin.scala._
import org.apache.tinkerpop.gremlin.orientdb.{OrientGraph, OrientGraphFactory}
import util._

trait Connection {

  def graphAsJava: OrientGraph

  def graph: ScalaGraph

}

object SingleConnection extends Connection {

  override val graphAsJava = new OrientGraphFactory(dbUrl, dbRoot, dbPassword).getNoTx()

  override val graph = graphAsJava.asScala

}

object PooledConnection extends Connection {

  val graphFactory = new OrientGraphFactory(dbUrl, dbRoot, dbPassword)
  graphFactory.setupPool(dbMaxPartitionSize, dbMaxPool)

  override def graphAsJava = graphFactory.getNoTx

  override def graph = graphAsJava.asScala

}
