package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, current_timestamp}
import service.DataPipeline
import org.apache.spark.internal.Logging

object NullCheck extends Logging {
  def nullCheck(df1cast:DataFrame,df2cast:DataFrame,nullPathClickstream:String,nullPathItemset:String):(DataFrame,DataFrame)={

    try {
      // drop the records where "id" and "item_id" are null
      val df1notnull = df1cast.na.drop(Seq("id"))
      val df2notnull = df2cast.na.drop(Seq("item_id"))

      // defining placeholders for both datasets where null values are present
      val placeholderClickstream = Map(
        "id" -> " ",
        "event_timestamp" -> "%",
        "device_type" -> "%",
        "session_id" -> "%",
        "visitor_id" -> "%",
        "item_id" -> "%",
        "redirection_source" -> "%"
      )
      val placeholderItemset = Map(
        "item_id" -> " ",
        "item_price" -> 0.0,
        "product_type" -> "%",
        "department_name" -> "%"
      )

      // filling all null values with defined placeholders
      val replacedNullClickstream = df1notnull.na.fill(placeholderClickstream)
      val replacedNullItemset = df2notnull.na.fill(placeholderItemset)
      replacedNullClickstream.show()
      replacedNullItemset.show()

      // store the null records in dataframes and into
      val nullRecordsClickstream = df1cast.filter(col("id").isNull)
      val nullRecordsItemset = df2cast.filter(col("item_id").isNull)

      nullRecordsClickstream.repartition(1).write.mode("overwrite").option("header","true").csv(nullPathClickstream)
      nullRecordsItemset.repartition(1).write.mode("overwrite").option("header","true").csv(nullPathItemset)

      (replacedNullClickstream,replacedNullItemset)
    }
    catch {
      case ex: Exception =>
        logInfo("An error occured due to null removel.",ex)

        (df1cast,df2cast)
    }
  }
}
