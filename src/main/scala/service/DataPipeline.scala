package service

import com.typesafe.config.{Config, ConfigFactory}
import constants.ApplicationConstants
import database.DatabaseWrite
import org.apache.spark.sql.SparkSession
import dataQuality.{ClickstreamItemPrice,ItemsetEventTimestamp}
import transform.{CastDataTypes, ConvertToLowercase, NullCheck, RemoveDuplicates, RenameColumn}

// here the main execution of pipeline starts
object DataPipeline {
  def dataPipeline(spark:SparkSession, config:Config, appConstants: ApplicationConstants):Unit={

    // reading the input files from clickstreamLocalConfig.conf
    val inputPath_clickstream = config.getString(appConstants.CLICKSTREAM_INPUT_PATH)
    val inputPath_itemset = config.getString(appConstants.ITEMSET_INPUT_PATH)

    // Write processed data to output path
    val outputPath = config.getString(appConstants.JOINED_DATASET)
    val nullPathClickstream = config.getString(appConstants.CLICKSTREAM_NULLS)
    val nullPathItemset = config.getString(appConstants.ITEMSET_NULLS)
    val duplicatesPathClickstream = config.getString(appConstants.CLICKSTREAM_DUPLICATES)
    val duplicatesPathItemset = config.getString(appConstants.ITEMSET_DUPLICATES)
    val invalidItemPrice = config.getString(appConstants.INVALID_ITEM_PRICE)
    val invalidEventTimestamp = config.getString(appConstants.INVALID_EVENT_TIMESTAMP)

    // calling FileReader to read both input files
    val clickstream_DF=FileReader.readDataFrame(spark,inputPath_clickstream)
    val itemSet_DF=FileReader.readDataFrame(spark,inputPath_itemset)

    // calling CastDataTypes object to cast the datatypes of columns
    val (df1cast,df2cast)=CastDataTypes.castDataTypes(clickstream_DF,itemSet_DF)

    // calling NullCheck object to remove columns where null values are present
    val (df1removenull,df2removenull)=NullCheck.nullCheck(df1cast,df2cast,nullPathClickstream,nullPathItemset)

    // calling RemoveDuplicates object to remove the columns where duplicate values are present
    val (df1duplicates,df2duplicates)=RemoveDuplicates.removeDuplicates(df1removenull,df2removenull,duplicatesPathClickstream,duplicatesPathItemset)

    // calling ConvertToLowercase object to convert all records of a particular column to lowercase
    val (df1lowercase,df2lowercase)=ConvertToLowercase.convertToLowercase(df1duplicates,df2duplicates)

    // calling RenameColumn object to rename column names to their meeaningful names
    val (df1rename,df2rename)=RenameColumn.renameColumn(df1lowercase,df2lowercase)

    // calling FileWriter object to join the two dataframes and write the final output to a csv file
    val joinedDF = FileWriter.fileWriter(df1rename,df2rename,outputPath)

    // data quality check for item_price(>200)----constant DQ_ITEM_PRICE_THRESHOLD
    ClickstreamItemPrice.clickstreamItemPrice(joinedDF,invalidItemPrice)

    // data quality check for event_timestamp()--------DQ_EVENT_TIMESTAMP_THRESHOLD
    ItemsetEventTimestamp.itemsetEventTimestamp(joinedDF,invalidEventTimestamp)

    // calling DatabaseWrite object to write the final dataframe to MySql table named "cdp"
    DatabaseWrite.writeToMySQL(joinedDF, "cdp", config, appConstants)
    // joinedDF.show()
  }
}
