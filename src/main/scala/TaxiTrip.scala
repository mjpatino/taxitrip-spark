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

  

  val sc = spark.sparkContext


  def countsByDate(df: DataFrame, save: Boolean = false, path:String): DataFrame = {

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


  def main(args: Array[String]): Unit = {
    

    val df = spark.read.parquet("/home/ec2-user/taxitrip-spark/data/fhvhv_tripdata/*.parquet")

    val baseBroadcastVar: Broadcast[Map[String, String]]= sc.broadcast(
      Map(
        "HV0002" -> "Juno",
        "HV0003" -> "Uber",
        "HV0004" -> "Via",
        "HV0005" -> "Lyft"
      ))

    val getBaseUDF = udf(
      (base_id: String) => baseBroadcastVar.value.getOrElse(base_id,base_id)
    )

    spark.udf.register("getBaseUDF", getBaseUDF)

    val df_2 = df.withColumn("base",call_udf("getBaseUDF", $"hvfhs_license_num"))

    baseBroadcastVar.unpersist()

    

    val _ = countsByDate(df,true,"/home/ec2-user/proccessed.parquet")
    
    spark.close()
  }
}