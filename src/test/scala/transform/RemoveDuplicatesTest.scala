package transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import utils.spark_readDF_config_test

class RemoveDuplicatesTest extends AnyFlatSpec {

  "RemoveDuplicates object" should "do the following"

  it should "remove duplicate values" in{
    val (clickstreamDF, itemsetDF) = spark_readDF_config_test.readTestDF()
    val duplicatesPathClickstream = ConfigFactory.load("test_application.conf").getString("output.sampleDuplicateClickstream")
    val duplicatesPathItemset = ConfigFactory.load("test_application.conf").getString("output.sampleDuplicateItemset")

    val (df1removeduplicates, df2removeduplicates) = RemoveDuplicates.removeDuplicates(clickstreamDF, itemsetDF, duplicatesPathClickstream, duplicatesPathItemset)

    assertResult(8)(df1removeduplicates.select("id").count())
    assertResult(8)(df2removeduplicates.select("item_id").count())

    df1removeduplicates.show()
    df2removeduplicates.show()
  }
}
