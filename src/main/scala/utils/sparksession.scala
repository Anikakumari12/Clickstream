package utils

import com.typesafe.config.Config
import constants.ApplicationConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparksession {
  def sparkSession(sparkConf:SparkConf, conf:Config, appConstants: ApplicationConstants):SparkSession= {
    // creates a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val logLevel = conf.getString(appConstants.SPARK_LOGLEVEL)
    spark.sparkContext.setLogLevel(logLevel)
    spark
  }
}
