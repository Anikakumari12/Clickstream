package transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import service.DataPipeline
import org.apache.spark.internal.Logging

object RemoveDuplicates extends Logging {
  def removeDuplicates(df1removenull:DataFrame,df2removenull:DataFrame,duplicatesPathClickstream:String,duplicatesPathItemset:String):(DataFrame,DataFrame)={
    try {
      df1removenull.show()
      df2removenull.show()
      // remove all duplicate records from columns "id" and "item_id"
      val df1Duplicates = df1removenull.dropDuplicates("id")
      val df2Duplicates = df2removenull.dropDuplicates("item_id")

      val duplicatesClickstream = df1removenull.except(df1Duplicates)
      val duplicatesItemset = df2removenull.except(df2Duplicates)

      duplicatesClickstream.repartition(1).write.mode("overwrite").option("header", "true").csv(duplicatesPathClickstream)
      duplicatesItemset.repartition(1).write.mode("overwrite").option("header", "true").csv(duplicatesPathItemset)

      (df1Duplicates, df2Duplicates)
    } catch {
      case e: Exception =>
        logInfo("An error occurred during duplicate removal.",e)
        // Returning original DataFrames as an example
        (df1removenull, df2removenull)
    }
  }
}
