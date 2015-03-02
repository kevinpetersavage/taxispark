import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.{DeleteDomainRequest, SelectRequest}

import scala.collection.JavaConversions._

object Dump {
  def main(args: Array[String]): Unit = {
    val db = new AmazonSimpleDBClient(new BasicAWSCredentials(args(0), args(1)))
    List("processed", "results").foreach(dumpPairs(db))
//    List("processed", "results").foreach(v => db.deleteDomain(new DeleteDomainRequest(v)))
  }

  def dumpPairs(db: AmazonSimpleDBClient)(domain: String): Unit = {
    println(domain + "=========")
    db.select(new SelectRequest("select * from " + domain)).getItems
      .map(item => (item.getName, item.getAttributes.map(att => att.getValue).mkString(",")))
      .map(println(_))
  }
}
