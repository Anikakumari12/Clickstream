package database

import com.typesafe.config.{Config, ConfigFactory}
import constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object DatabaseWrite extends Logging{
  def writeToMySQL(dataFrame: DataFrame, tableName: String, config:Config, appConstants: ApplicationConstants): Unit = {
    try
    {
      val Url = config.getString(appConstants.JDBC_URL);
      val User = config.getString(appConstants.JDBC_USER);
      val Password = config.getString(appConstants.JDBC_PASSWORD);

      dataFrame.write // initiates the process of writing the DataFrame to MySql
        .format("jdbc") // Sets writing format to JDBC
        .mode("overwrite") // Overwrites existing data in target
        .option("driver", "com.mysql.cj.jdbc.Driver") // Specifies MySQL JDBC driver
        .option("url", Url) // Sets MySQL database URL
        .option("dbtable", tableName) // Specifies target table name
        .option("user", User) // Provides MySQL username
        .option("password", Password) // Provides MySQL password
        .save() // Executes DataFrame write to MySQL
      }
      catch {
        case e:Exception=>
          logInfo("An error occurred during loading the data to MySQL table",e)
      }
  }
}
