//package dataevents
//
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions._
//
//object data_cleaning {
//
////  def readConfig():SparkConf= {
////    val config = ConfigFactory.load("application.conf")
////    val sparkAppName = config.getString("spark.appName")
////    val sparkMaster = config.getString("spark.master")
////    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
////  }
//
////  def readDataFrame(spark:SparkSession,inputPath:String):DataFrame={
////    spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
////  }
//
////  def removeNullRecords(df: DataFrame, columns: Seq[String]): DataFrame = {
////    df.na.drop(columns)
////  }
////
////  def removeDuplicates(df: DataFrame, columns: Seq[String]): DataFrame = {
////    df.dropDuplicates(columns)
////  }
////
////  def convertToLowercase(df: DataFrame, column: String): DataFrame = {
////    df.withColumn(column, lower(col(column)))
////
////  }
//
//  def main(args: Array[String]): Unit = {
//    // reading configuration file----application.config
//
////    val sparkConf = readConfig()
////    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
////    val logLevel = ConfigFactory.load("application.conf").getString("spark.logLevel")
////    spark.sparkContext.setLogLevel(logLevel)
//
//    // Read clickstream data from input paths
////    val inputPath1 = ConfigFactory.load("application.conf").getString("input.path1")
////    val inputPath2 = ConfigFactory.load("application.conf").getString("input.path2")
////
////
////    // Read both CSV files into a DataFrame
////    val df1=readDataFrame(spark,inputPath1)
////    val df2 =readDataFrame(spark,inputPath2)
//
//    // Cast to the desired data types
////    val df1cast=df1.withColumn("event_timestamp",
////      to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))
////    val df2cast:DataFrame=df2.withColumn("item_price", col("item_price").cast("Double"))
//
//    // Rename to meaningful column names
////    val df1rename: DataFrame = df1cast.withColumnRenamed("id", "Entity_id")
////      .withColumnRenamed("device_type", "device_type_t")
////      .withColumnRenamed("session_id", "visitor_session_c")
////      .withColumnRenamed("redirection_source", "redirection_source_t")//.printSchema()
////    val df2rename :DataFrame = df2cast.withColumnRenamed("item_price", "item_unit_price_a")
////      .withColumnRenamed("product_type", "product_type_c")
////      .withColumnRenamed("department_name", "department_n")
//
//    // Remove records with null values
////    val df1notnull = removeNullRecords(df1rename, Seq("Entity_id"))
////    val df2notnull = removeNullRecords(df2rename, Seq("item_id"))
//
//    // Remove duplicate values
////    val df1Duplicates = removeDuplicates(df1notnull, Seq("Entity_id"))
////    val df2Duplicates = removeDuplicates(df2notnull, Seq("item_id"))
//
//    // Convert everything to lower case
////    val df1lowercase = convertToLowercase(df1Duplicates, "redirection_source_t")
////    val df2lowercase = convertToLowercase(df2Duplicates, "department_n")
//
//    // Print both schemas
////    df1lowercase.printSchema()
////    df2lowercase.printSchema()
//
//    // Join both the dataframes
////    val finalDF: DataFrame = df1lowercase.join(df2lowercase, Seq("item_id"))
////
////    // Show the final DataFrame
////    finalDF.show()
////
////    // Write processed data to output path
////    val outputPath = ConfigFactory.load("application.conf").getString("output.path")
////
////    // Write the final dataframe to a csv file
////    finalDF.repartition(1).write.option("header","true").csv(outputPath)
//
////    spark.stop()
// }
//}
////import org.apache.spark.sql._
////import org.apache.spark.sql.functions._
////
////import com.typesafe.config.ConfigFactory
////import org.apache.spark.SparkConf
////import org.apache.spark.sql.SparkSession
////
////
////object Target {
////  def main(args: Array[String]): Unit = {
////    val config = ConfigFactory.load("application.conf")
////    val sparkMaster = config.getString("spark.master")
////    val sparkConf = new SparkConf().setAppName("ClickstreamDataPipeline").setMaster(sparkMaster)
////    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
////
////
//////    val sparkConf = new SparkConf().setAppName("ClickstreamDataPipeline")
//////    val spark = SparkSession.builder().config(sparkConf).getOrCreate().setMaster(sparkMaster)
////    //    val spark = SparkSession.builder()
//////      .master("local")
//////      .appName("ClickstreamDataPipeline")
//////      .getOrCreate()
//////    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.1")
//////    spark.sparkContext.setLogLevel("error")
////
////
////    val inputPath1 = config.getString("input.path1")
////    val inputPath2 = config.getString("input.path2")
////    val outputPath=config.getString("output.path")
////
////    //    val folderPath1 = "C:\\Users\\Hp\\OneDrive\\Desktop\\PipeLineData\\clickstream_log (1).csv"
//////    val folderPath2 = "C:\\Users\\Hp\\OneDrive\\Desktop\\PipeLineData\\item_data (1).csv"
////    // Read all CSV files from the folder into a DataFrame
////
////    val df1: DataFrame = spark.read
////      .option("header", "true") // If the CSV files have a header row, set this option to "true"
////      .option("inferSchema", "true") // Infers the data types of columns
////      .csv(inputPath1)
////    val df2: DataFrame = spark.read
////      .option("header", "true") // If the CSV files have a header row, set this option to "true"
////      .option("inferSchema", "true") // Infers the data types of columns
////      .csv(inputPath2)
////
////    val df1WithoutDuplicates = df1.dropDuplicates("id")//-------drop duplicates from id column
////    val df2WithoutDuplicates = df2.dropDuplicates("item_id")
////
////   val df1cast= df1WithoutDuplicates.withColumn("event_timestamp",
////      to_timestamp(col("event_timestamp")))
////    //val df1cast= df1WithoutDuplicates.withColumn("event_timestamp", to_timestamp($"date_string", "MM/dd/yy HH:mm"))
////
////    val df2cast= df2WithoutDuplicates.withColumn("item_price",col("item_price").cast("Double"))
////
////     val df1rename=df1cast.select(col("id").as("Entity_id"),col("event_timestamp"),
////      col("device_type").as("device_type_t"),
////      col("session_id").as("visitor_session_c"),col("visitor_id"),col("item_id"),
////      col("redirection_source").as("redirection_source_t") )
////    val df2rename=df2cast.select(col("item_id"),
////      col("item_price").as("item_unit_price_a"),
////      col("product_type").as("product_type_c"),
////      col("department_name").as("department_n")).printSchema()
////
////    df1rename.write.option("header","true").csv(outputPath)
////
////    //
//////
////    spark.stop()
////}
////}
////package transformation
////
////import org.apache.spark.sql._
////import org.apache.spark.sql.functions._
////
////object data_cleaning {
////  def main(args: Array[String]) {
////    val spark = SparkSession.builder.master("local[*]").appName("data_cleaning").getOrCreate()
////    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
////    val sc = spark.sparkContext
////    sc.setLogLevel("ERROR")
////    import spark.implicits._
////    import spark.sql
////
////    spark.stop()
////  }
////}
