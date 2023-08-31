package service

import org.apache.spark.sql._
import service.DataPipeline
import org.apache.spark.internal.Logging

object FileWriter extends Logging {
  def fileWriter(df1rename:DataFrame, df2rename:DataFrame,outputPath:String): DataFrame = {
    try {
      df1rename.printSchema()
      df2rename.printSchema()

      // Join both the dataframes
      val finalDF: DataFrame = df1rename.join(df2rename, Seq("item_id"))

      // Show the final DataFrame
      finalDF.show()

      // Write the final dataframe to a csv file
      finalDF.repartition(1).write.mode("overwrite").option("header", "true").csv(outputPath)
      finalDF
    }
    catch {
      case e: Exception =>
        logInfo(s"No data is there in dataframe to load in MySQL table",e)
        // we can handle the error here, such as returning an empty DataFrame or rethrowing the exception
        // Returning one of the input DataFrames as an example
        null
    }
  }
}
