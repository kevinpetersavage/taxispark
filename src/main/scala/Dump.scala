import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.{SelectResult, DeleteDomainRequest, SelectRequest}

import scala.collection.JavaConversions._

object Dump {
  def main(args: Array[String]): Unit = {
    val db = new AmazonSimpleDBClient(new BasicAWSCredentials(args(0), args(1)))
    List("processed", "results").foreach(dumpPairs(db))
//    List("processed", "results").foreach(v => db.deleteDomain(new DeleteDomainRequest(v)))
  }

  def dumpPairs(db: AmazonSimpleDBClient)(domain: String): Unit = {
    println(domain + "=========")

    var selectResult = db.select(new SelectRequest("select * from " + domain, true))
    selectResult.getItems
      .map(item => (item.getName, item.getAttributes.map(att => att.getValue).mkString(",")))
      .map(println(_))

    while (selectResult.getNextToken != null){
      val selectRequest: SelectRequest = new SelectRequest("select * from " + domain, true)
      selectRequest.setNextToken(selectResult.getNextToken)
      selectResult = db.select(selectRequest)
      selectResult.getItems
        .map(item => (item.getName, item.getAttributes.map(att => att.getValue).mkString(",")))
        .map(println(_))
    }
  }
}
