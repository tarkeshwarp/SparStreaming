package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions 


object secondexample {
  
   def main(args:Array[String]):Unit=
   
   {
     
      val conf = new SparkConf().setAppName("all joins").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("error")


  
}
   
}   