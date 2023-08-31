package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import utils.spark_readDF_config_test
import org.apache.spark.sql.functions.{col, lower, upper}

class ConvertToLowercaseTest extends AnyFlatSpec {

  "ConvertToLowercase object" should "do the following"

  it should "convert the specified column records to lowercase" in{
    val (clickstreamDF, itemsetDF) = spark_readDF_config_test.readTestDF()
    val (df1lowercase, df2lowercase) = ConvertToLowercase.convertToLowercase(clickstreamDF, itemsetDF)

    assertResult(10)(df1lowercase.select("redirection_source").count())
    assertResult(10)(df2lowercase.select("department_name").count())

    df1lowercase.show()
    df2lowercase.show()
  }
}
