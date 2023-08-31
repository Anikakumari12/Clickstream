//package transform
//
//import org.apache.spark.sql.DataFrame
//import org.scalatest._
//import org.scalatest.flatspec.AnyFlatSpec
//import database.DatabaseWrite
//
//class DatabaseWriteTest extends AnyFlatSpec {
//  val fileWriter= new FileWriterTest
//  var final_test_table: Unit = _
//  "DatabaseWrite object" should "do the following"
//
//  it should "write the output dataframe into MYSQL table" in{
//    final_test_table = DatabaseWrite.writeToMySQL(fileWriter.final_test_DF,"sample_cdp")
//  }
//}
