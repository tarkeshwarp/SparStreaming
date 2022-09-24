package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Sparknull {
  
  case class test1(col1 :String,col2 :String,col3 :String,col4 :String,col5 :String)
  
  def main(args:Array[String]):Unit={
    
    
    val conf = new SparkConf().setAppName("SCDtype2").setMaster("local")
    
    val sc = new SparkContext(conf)
    
     sc.setLogLevel("error")
     
     val spark = SparkSession.builder().config(conf).getOrCreate()
     
     import spark.sqlContext.implicits._
     
     val readdf = spark.read.format("csv")
     // .schema(test1) 
     .option("header", "true")
    // .option("nullValue", "null")
     .load("file:///E:/Hadoop_Software/data/Realtimedata/Datanull.txt")
     
     readdf.persist()
     
     readdf.show()
    
     println(" Column a is not null ")
     readdf.filter("a is not null").show()
     
     
     println(" Column a is  null ")
     
     readdf.where("a is null").show()
    
  }
  
  
}