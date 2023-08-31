package dataQuality

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import constants.ApplicationConstants

object ClickstreamItemPrice {

  def clickstreamItemPrice(dfWithPrices: DataFrame, invalidPrice:String): Unit = {
    val appConstants = new ApplicationConstants
    val lowerLimit = 0.0 // Replace with your lower limit
    val upperLimit = appConstants.DQ_ITEM_PRICE_THRESHOLD  // Replace with your upper limit

    // Filter out values outside the range (invalid data)
    val invalidData = dfWithPrices.filter(col("item_unit_price_a") < lowerLimit || col("item_unit_price_a") > upperLimit)

    // Save invalid data to a separate file (CSV format)
    invalidData.write.mode("overwrite").option("header", "true").csv(invalidPrice)

    // Summary of Price Range for valid data
    val minPrice = dfWithPrices.agg(min("item_unit_price_a")).collect()(0)(0)
    val maxPrice = dfWithPrices.agg(max("item_unit_price_a")).collect()(0)(0)
    println(s"Minimum item price: $minPrice")
    println(s"Maximum item price: $maxPrice")

    // Logging and Reporting
    // You can log the results or generate reports to document the findings of your data quality checks.
  }
}
