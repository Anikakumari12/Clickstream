package transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import utils.spark_readDF_config_test

class NullCheckTest extends AnyFlatSpec {

  "NullCheck object" should "do the following"

  it should "remove null values" in{
    val (clickstreamDF, itemsetDF) = spark_readDF_config_test.readTestDF()

    val nullPathClickstream = ConfigFactory.load("test_application.conf").getString("output.sampleNullClickstream")
    val nullPathItemset = ConfigFactory.load("test_application.conf").getString("output.sampleNullItemset")

    val (df1removenull, df2removenull) = NullCheck.nullCheck(clickstreamDF, itemsetDF, nullPathClickstream, nullPathItemset)

    assertResult(9)(df1removenull.select("id").count())
    assertResult(10)(df2removenull.select("item_id").count())

    df1removenull.show()
    df2removenull.show()
  }
}
