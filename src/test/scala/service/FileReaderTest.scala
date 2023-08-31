package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import utils.spark_readDF_config_test

class FileReaderTest extends AnyFlatSpec {

  "FileReader object" should "do the following"

  it should "read the files in the path" in {
    val spark = spark_readDF_config_test.sparkSession()

    val inputPath_clickstream = ConfigFactory.load("test_application.conf").getString("input.sample_path1")
    val inputPath_itemset = ConfigFactory.load("test_application.conf").getString("input.sample_path2")

    val clickstream_test_DF = FileReader.readDataFrame(spark,inputPath_clickstream)
    val itemset_test_DF = FileReader.readDataFrame(spark,inputPath_itemset)

    assertResult(9)(clickstream_test_DF.count())
    assertResult(10)(itemset_test_DF.count())

    clickstream_test_DF.show()
    itemset_test_DF.show()
  }
}
