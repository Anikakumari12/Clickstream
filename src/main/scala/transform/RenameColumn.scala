package transform

import org.apache.spark.sql._
import service.DataPipeline
import org.apache.spark.internal.Logging

object RenameColumn extends Logging {
  def renameColumn(df1lowercase:DataFrame,df2lowercase:DataFrame):(DataFrame,DataFrame)={
    try
    { // renaming column names to their meaningful names of clickstream dataset
      val df1rename: DataFrame = df1lowercase.withColumnRenamed("id", "Entity_id")
        .withColumnRenamed("device_type", "device_type_t")
        .withColumnRenamed("session_id", "visitor_session_c")
        .withColumnRenamed("redirection_source", "redirection_source_t") //.printSchema()

      // renaming column names to their meaningful names of item dataset
      val df2rename: DataFrame = df2lowercase.withColumnRenamed("item_price", "item_unit_price_a")
        .withColumnRenamed("product_type", "product_type_c")
        .withColumnRenamed("department_name", "department_n")
      (df1rename, df2rename)
    }
    catch {
      case e: Exception =>
        logInfo("An error occurred during renaming the columns.", e)
        // Returning original DataFrames as an example
        (df1lowercase, df2lowercase)
    }
  }
}
