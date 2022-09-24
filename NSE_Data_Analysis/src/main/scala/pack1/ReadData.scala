package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object ReadData {
  
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("NSE").setMaster("local")
    
    val sc = new SparkContext(conf)
    
     sc.setLogLevel("error")
     
     val spark = SparkSession.builder().config(conf).getOrCreate()
     
     val df = spark.read.format("csv").option("header", "false").load("file:///E:/Hadoop_Software/data/NSE_Data/EOD.csv")
     
     df.show()
     
  val sumdf=  df.groupBy("_c0").agg(sum("_c2").alias("sum1"))
  
  val ddd= Window.partitionBy("_c0").orderBy("sum1")
  
  sumdf.withColumn("rank", row_number.over(ddd)).show()
  
  println("end of the project")
  
  }
  
}