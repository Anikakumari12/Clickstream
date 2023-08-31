package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import utils.spark_readDF_config_test
import org.apache.spark.sql.types.{StringType,TimestampType, DoubleType}

class CastDataTypesTest extends AnyFlatSpec {

  "CastDataTypes object" should "do the following"

  it should "cast datatypes of columns to desired datatypes" in{
    val (clickstreamDF,itemsetDF)=spark_readDF_config_test.readTestDF()
    val (df1cast,df2cast) = CastDataTypes.castDataTypes(clickstreamDF,itemsetDF)

    val str=df1cast.schema("event_timestamp").dataType
    val double=df2cast.schema("item_price").dataType
    // Assert datatype for timestamp column using assertResult
    assertResult(str)(StringType)

    // Assert datatype for double column using assertResult
    assertResult(double)(DoubleType)

//    df1cast.show()
//    df2cast.show()
  }
}
