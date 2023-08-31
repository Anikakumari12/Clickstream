package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import utils.spark_readDF_config_test

class RenameColumnTest extends AnyFlatSpec {

  "RenameColumn object" should "do the following"

  it should "rename columns" in{
    val (clickstreamDF, itemsetDF) = spark_readDF_config_test.readTestDF()
    val (df1rename, df2rename) = RenameColumn.renameColumn(clickstreamDF, itemsetDF)

    val clickstreamExpected=Array("Entity_id","event_timestamp","device_type_t","visitor_session_c","visitor_id","item_id","redirection_source_t")
    val itemset_expected=Array("item_id","item_unit_price_a","product_type_c","department_n")

    assertResult(clickstreamExpected)(df1rename.columns)
    assertResult(itemset_expected)(df2rename.columns)

    df1rename.show()
    df2rename.show()
  }
}
