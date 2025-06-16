package taxitrip

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.broadcast.Broadcast


object TaxiTrip extends TaxiTripInterface {
  
  

  val spark = SparkSession
              .builder
              .appName("TaxiTrip Data")
              .getOrCreate()
  
  import spark.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  

  val sc = spark.sparkContext


  def aggByDate(df: DataFrame, save: Boolean = false, path:String): DataFrame = {

    val dailyCounts = df
                      .withColumn("year", year($"pickup_datetime"))
                      .withColumn("month", month($"pickup_datetime"))
                      .withColumn("day", dayofmonth($"pickup_datetime"))
                      .groupBy($"year",$"month", $"day")
                      .agg(
                        count($"hvfhs_license_num"),
                        sum($"trip_miles"),
                        sum($"trip_time"),
                        sum($"base_passenger_fare"),
                        sum($"tolls"),
                        sum($"bcf"),
                        sum($"sales_tax"),
                        sum($"airport_fee"),
                        sum($"tips"),
                        sum($"driver_pay"),
                        count( when($"airport_fee" > 0, 1) )
                      )
                      .orderBy($"year",$"month", $"day")
    // dailyCounts.explain()
    if (save) dailyCounts.write.parquet(path)
    // dailyCounts.show()
    dailyCounts
  }

  def statsByLocation(df: DataFrame, colGroupBy: String): DataFrame = {
    df.groupBy($"PULocationID").agg(
      count($"PULocationID"),
      avg($"trip_time"),
      avg($"trip_miles")
    )
  }

  // def loadAndSave(input_file:String, output_file:String) : Unit = {
    
  //   val df = spark.read.parquet(input_file)
  //   if (df.schema("airport_fee").dataType == IntegerType) {
  //     df.withColumn("airport_fee", col("airport_fee").cast(DoubleType)).write.parquet(output_file)
  //   } 
  //   else
  //     df.write.parquet(output_file)
  // }
    def loadAndSave(input_file:String, output_file:String, save:Boolean) : Unit = {
    
    val df = spark.read.parquet(input_file)


    val missingCols: Map[String, DataType] = Map(
      "DOLocationID" -> LongType,
      "PULocationID" -> LongType,
      "airport_fee" -> DoubleType
      // "wav_match_flag" -> BYTE_ARRAY | INT32
    )

    def ensureColumnsWithTypes(df: DataFrame, expected: Map[String, DataType]): DataFrame = {
      expected.foldLeft(df) { case (tempDf, (colName, dataType)) =>
        if (tempDf.columns.contains(colName)) tempDf.withColumn(colName, col(colName).cast(dataType)) // TODO: Validar optimizacion en Catalyst para CAST
        else tempDf.withColumn(colName, lit(null).cast(dataType))
      }
    }

    val finalDf : DataFrame = ensureColumnsWithTypes(df, missingCols)

    if (save) {
      finalDf.write.parquet(output_file)
    }

    finalDf
  }

  def parquetSchema( path: String, save: Boolean ): Unit = {

    import org.apache.hadoop.fs.{FileSystem, Path}

    val fs = FileSystem.get( sc.hadoopConfiguration )

    val dirPath = new Path( s"file://$path"  )

    val files = fs.listStatus(dirPath).map{
      f => f.getPath().toString()
    }.toSeq

    files.foreach(println)
    println(files.size)


    files.foreach{ f => loadAndSave(f,f.replace("raw","staging"), save)}

  }


  def main(args: Array[String]): Unit = {

    val file_folder: String = "/home/ec2-user/raw/fhvhv_tripdata"

    parquetSchema( file_folder, true )



    val df = spark.read.parquet("/home/ec2-user/staging/fhvhv_tripdata/*.parquet")

    val _ = aggByDate(df,true,"/home/ec2-user/aggByDate.parquet")

    
    spark.close()
  }
}