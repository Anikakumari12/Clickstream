//package database
//
//import org.apache.spark.sql._
//import org.apache.spark.sql.functions._
//import utils.sparksession
//
//object DatabaseRead {
//  val spark=sparksession.sparkSession()
//  def readFromMySQL(dataFrame: DataFrame, tableName: String): Unit =
//    {
//      val readDataFromSQL = spark.read
//        .format("jdbc")
//        .option("driver", "com.mysql.cj.jdbc.Driver")
//        .option("url", constant.jdbcUrl)
//        .option("dbtable","CDP")
//        .option("user", constant.jdbcUser)
//        .option("password", constant.jdbcPassword)
//        .load()
//    }
//}
