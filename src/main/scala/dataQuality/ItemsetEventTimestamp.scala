package dataQuality

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import constants.ApplicationConstants

object ItemsetEventTimestamp {
  def itemsetEventTimestamp(dfWithTimestamp:DataFrame, invalidevents:String): Unit = {
    // Define lower and upper limits for timestamp range
    val appConstants = new ApplicationConstants
    val lowerLimit = "2020-01-01 00:00:00" // Replace with your lower limit
    val upperLimit = appConstants.DQ_EVENT_TIMESTAMP_THRESHOLD // Replace with your upper limit

    // Filter out timestamps outside the range (invalid data)
    val invalidTimestampData = dfWithTimestamp.filter(col("event_timestamp") < lowerLimit || col("event_timestamp") > upperLimit)

    // Save invalid timestamp data to a separate file (CSV format)
    invalidTimestampData.write.mode("overwrite").option("header", "true").csv(invalidevents)

    // Summary of Timestamp Range for valid data
    val minTimestamp = dfWithTimestamp.agg(min("event_timestamp")).collect()(0)(0)
    val maxTimestamp = dfWithTimestamp.agg(max("event_timestamp")).collect()(0)(0)
    println(s"Minimum timestamp: $minTimestamp")
    println(s"Maximum timestamp: $maxTimestamp")
  }
}
