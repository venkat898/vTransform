package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import java.math.BigDecimal
import java.net.URI
import java.sql.Date
import java.util._
import java.time.format.DateTimeFormatter

object S3Transformation {
	var spark: SparkSession = _
	def main(args: Array[String]) = {
	  spark = SparkSession
						.builder()
						.appName("S3 Transformation")
						.enableHiveSupport()
						.getOrCreate()

		println("Application Id : " + spark.sparkContext.applicationId)
	  if(dataTransformation(args)) {
			println("Process completed succesfully!!")
		}
		else {
			println("Process failed!!")
		}
  }

  def dataTransformation(consoleArgs: Array[String]): Boolean = {    
		//Define the Constant Values
		if (consoleArgs.length.toInt != 5) throw new Exception("Please pass (5) arguments. Current No. of arguments passed : " + consoleArgs.length.toInt);

		var s3OutputPath: String = consoleArgs(0).toString
		var glueDatabase: String = consoleArgs(1).toString
		var glueObjectName : String = consoleArgs(2).toString
		var isIncremental : String = consoleArgs(3).toString
		var transformQuery : String = consoleArgs(4).toString

		//get the spark Context
		val sc = spark.sparkContext

		var toOverwrite = if (isIncremental.equalsIgnoreCase("false")) "overwrite" else "append"

		val df = spark.sql(transformQuery)

		//Create the path with the output
		df.write.option("path",s3OutputPath).mode(toOverwrite).saveAsTable(glueDatabase + "." + glueObjectName)
		
		spark.stop()
		return true
  }
}