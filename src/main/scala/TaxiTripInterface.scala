package taxitrip

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkContext
import javax.xml.crypto.Data

trait TaxiTripInterface {
  val spark: SparkSession
  val sc: SparkContext

  def countsByDate(df: DataFrame, save: Boolean = false, path:String): DataFrame

  def statsByLocation(df: DataFrame, colGroupBy: String): DataFrame



  def main(args: Array[String]): Unit
}