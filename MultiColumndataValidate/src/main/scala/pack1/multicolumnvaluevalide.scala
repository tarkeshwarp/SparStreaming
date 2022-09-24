package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object multicolumnvaluevalide {
  
  def main(args:Array[String]):Unit={

 // val validateColumns = udf((date: String, count: String, name: String)){
 // error logic using the 3 column strings
//  err_val

   val conf = new SparkConf().setAppName("SCDtype2").setMaster("local")
    
    val sc = new SparkContext(conf)
    
     sc.setLogLevel("error")
     
     val spark = SparkSession.builder().config(conf).getOrCreate()
     
     import spark.sqlContext.implicits._
     
     
     val readdf = spark.read.format("csv").option("Header", "True").load("file:///E:/Hadoop_Software/data/Realtimedata/Add_Data.txt")
     
     
     val addressisnull= readdf.filter(readdf("custid").isNull).select("custid", "products").show(false)
     
     
   def isNullFunction(value: String): Boolean = {
    if ( value == null ) {
        return true
    }
    return false
}


val isNullUDF = udf(isNullFunction _)

val df1= readdf.withColumn("IsNullUDF", isNullUDF(col("permanentAddress")))
                .withColumn("IsNullUDF", isNullUDF(col("custid")))
        .show()   
   
  // val errordf= finaldf.withColumn("error", col)
  
  
  
  
  }
  
  //val checkedDf = input.withColumn("ERROR_COMMENTS", validateColumns($"date", $"count", $"name"))

  
  
  
}