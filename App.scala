package org.example
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.rdd.RDD._
import org.apache.spark.SparkContext._

import java.lang.NullPointerException
import scala.collection.immutable
import scala.{::, Nil}


/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\Spark\\spark-3.0.0-bin-hadoop3.2")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    var sc = spark.sparkContext

    var conf = new SparkConf()

    conf.set("spark.sql.parquet.compression.codec","snappy")
    conf.set("spark.sql.parquet.writeLegacyFormat","true")

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    var inputFile = "D:\\DataEngg\\Datasets\\superstore.csv"


    var salesSchema = StructType(
      StructField("OrderDate", DateType, false) ::
        StructField("Sales", DoubleType, false)  ::
        StructField("UnitPrice", DoubleType, false) ::
        StructField("Region", StringType, false) ::
        StructField("ProductCategory", StringType, false) ::
        StructField("ProductName", StringType, false) :: Nil)

    var df = spark.read.format("csv").option("header", "true")
      .option("delimiter", ",").option("DateFormat","mm-dd-yyyy").schema(salesSchema).load(inputFile)
    df.createOrReplaceTempView("salesData")

    //FlatMap - if you have a dataset with array, it converts each elements in a array as a row.

    // 1 - Way
    var dfProdName = spark.sql("select ProductName from salesData")
    var df_t_flatMap = dfProdName.rdd.flatMap(f=>f.toString().split(" "))
//    df_t_flatMap.foreach(f=>println(f))

    // 2 - Way
    // val df2=df.flatMap(f=> f.getSeq[String](1).map((f.getString(0),_,f.getString(2))))
    //    .toDF("Name","language","State")

//    import spark.implicits._
//    try {
//      val df2 = df.flatMap(f => f.getSeq[String](3)).flatMap(f => f.split(" ")).show(7)
//    }catch {
//      case npe : SparkException => println("Null")
//    }

    // 2. Map - Applies transformation function on dataset and returns same number of elements in distributed dataset.

    var dfUnitPrice = df.select("UnitPrice")
//    var dfUnitPrice = spark.sql("select UnitPrice from salesData")
    var df_t_map = dfUnitPrice.rdd.map(f => f.getDouble(0) * 100)
//    df_t_map.foreach(f=>println(f))

    // 3. Filter

    var dfFilter = df.filter("ProductName is not null")
//    dfFilter.show(10)

//    val df2 = df.rdd.flatMap(f => f.getSeq[String](5)).flatMap(f => f.split(" ")).foreach(f=>println(f))

    // 4. Aggregate

    var dfAgg = df.select("Region", "UnitPrice").filter("Region is not null and UnitPrice is not null")
    var dfAggSeq = Seq(dfAgg)
    var df_agg = sc.parallelize(dfAggSeq)
//    df_agg.aggregate()

   // 5. ReduceByKey

//    var dfRedByKey = spark.sql("select Region, ProductName from salesData")
var dfRedByKey = df.select("Region","ProductName")
    var dfRed = dfRedByKey.rdd.map(f=>(f.getString(0), f.getString(1)))
    var dfRedBy = dfRed.reduceByKey(_+_)
    dfRedBy.collect().foreach(f=>println(f))
    // Dataframe and Dataset[Row] are different

    //6. groupByKey
    var dfGroup = dfRed.groupByKey().collect()
    dfGroup.foreach(f=> println(f))



//    val data = sc.parallelize(Array(("C",3),("A",1),("B",4),("A",2),("B",5)))
//    data.collect()




  }
}