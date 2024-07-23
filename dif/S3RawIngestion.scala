package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import java.math.BigDecimal
import java.net.URI
import java.sql.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import org.apache.spark.sql.types


object S3RawIngestion {
	var spark: SparkSession = _
	def main(args: Array[String]) = {
	  spark = SparkSession
						.builder()
						.appName("S3 Raw Ingestion")
						.enableHiveSupport()
						.getOrCreate()

		println("Application Id : " + spark.sparkContext.applicationId)
	  if(dbmsDataExtract(args)) {
			println("Process completed succesfully!!")
		}
		else {
			println("Process failed!!")
			throw new Exception("Process failed!!")
		}
  }

	def dbmsDataExtract(consoleArgs: Array[String]): Boolean = {
		//Define the Constant Values
		if (consoleArgs.length.toInt != 17) throw new Exception("Please pass (17) arguments. Current No. of arguments passed : " + consoleArgs.length.toInt);

		var URL: String = consoleArgs(0).toString
		var username: String = consoleArgs(1).toString
		var password: String =  consoleArgs(2).toString
		var queryForDataFetch: String = consoleArgs(3).toString

		var s3OutputPath: String = consoleArgs(4).toString
		var toBeArchived: String = consoleArgs(5).toString
		var glueDatabase: String = consoleArgs(6).toString
		var rawObjectName : String = consoleArgs(7).toString
		var glueObjectName : String = consoleArgs(8).toString
		var DBConnectionType : String = consoleArgs(9).toString
		var isIncremental : String = consoleArgs(10).toString
		var sourcePrimaryKey : String = consoleArgs(11).toString
		var targetPartitionColumn : String = consoleArgs(12).toString
		var noOfSourcePartitions : Int = consoleArgs(13).toInt
		var noOfTargetPartitions : Int = consoleArgs(14).toInt
		var auditQuery : String = consoleArgs(15).toString
		var requestId : Int = consoleArgs(16).toInt

		//var jdbcDriver: String = ""
		var fetchSizeVal: Int = 100000

		//get the spark Context
		val sc = spark.sparkContext

		//var nls_param: String = ""

		val (jdbcDriver,nls_param) = DBConnectionType match {
		  case "MYSQL" => ("com.mysql.jdbc.Driver","")
		  case "SAP" => ("com.sap.db.jdbc.Driver","")
		  case "MSSQL" => ("com.microsoft.sqlserver.jdbc.SQLServerDriver","")
		  case "POSTGRES" => ("org.postgresql.Driver","")
		  case "ORACLE" => ("oracle.jdbc.OracleDriver","ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD hh24:mi:ss.ff'")
		}

		var isArchive = if (toBeArchived.equalsIgnoreCase("false")) false else true
		var toOverwrite = if (isIncremental.equalsIgnoreCase("false")) "overwrite" else "append"
		
		var df: DataFrame = null
		var distinctCount: Long = 0
		
		if (isIncremental.equalsIgnoreCase("true") && spark.catalog.tableExists(glueDatabase + "." + glueObjectName)) {
		  
		  println("Performing a incremental load!!")
			val format = new java.text.SimpleDateFormat("YYYYMMdd_HHmmsss")
			
		  //create a landing dataframe on raw layer and move the raw data to landing.
		  var landingS3Path = s3OutputPath.replace("-raw-","-landing-")
		  val landFs = FileSystem.get(new URI(landingS3Path),sc.hadoopConfiguration)
		  landFs.delete(new Path(landingS3Path), true)
		  copyFiles(sc,s3OutputPath,landingS3Path.substring(0,landingS3Path.lastIndexOf("/")),false,true)
		  println(landingS3Path)
			val landingDf = spark.read.parquet(landingS3Path)
			
			//get the max updated timestamp on raw layer
			var maxUpdatedTimestamp = landingDf.selectExpr("max(update_timestamp) as maxUpdatedTimestamp").first().getTimestamp(0)
			
		  val maxTimestamp = maxUpdatedTimestamp.toString
			
		  println(maxTimestamp)
		  
			//get columns from landing df
			val cols = landingDf.columns.toList
			
			//using the max timestamp fetch the latest records from raw layer using below query
			//queryForDataFetch = "SELECT " + cols +" FROM " + rawObjectName + " WHERE update_timestamp > '" + maxTimestamp + "'"
			queryForDataFetch = "SELECT * FROM " + rawObjectName + " WHERE update_timestamp > '" + maxTimestamp + "'"
			val incrDf = spark.read.format("jdbc").option("driver", jdbcDriver).option("url", URL).option("user", username).option("password", password)
			            .option("sessionInitStatement",nls_param).option("query", queryForDataFetch).option("fetchsize", fetchSizeVal).option("inferSchema", "true").load()
			
			//get new columns and deleted columns
		  val newCols = incrDf.columns.toList.filterNot(landingDf.columns.toSet)
		  val delCols = landingDf.columns.toList.filterNot(incrDf.columns.toSet)
		  
		  //merge columns from both dataframes into a set
		  val mergedCols = landingDf.columns.toSet ++ incrDf.columns.toSet
			
			//create a view on landing dataframe
			val landingView = "landingTable" + format.format(java.util.Calendar.getInstance().getTime())
			landingDf.createOrReplaceTempView(landingView)
			
			//create a view on incremental dataframe
			val incrView = "IncrTable" + format.format(java.util.Calendar.getInstance().getTime())
			incrDf.createOrReplaceTempView(incrView)
			
			if(incrDf.rdd.isEmpty()) {
			  println("No new increments!!")
			  return true
			}
			
			//remove older records from landing df and insert new records
			val query = "select * from " + landingView + " where  " + sourcePrimaryKey + " not in (select " + sourcePrimaryKey + " from " + incrView + ")"
			//df = spark.sql(query).union(incrDf)
			val tempDf = spark.sql(query)
			
			//incase of new columns, add nulls to existing data
			//incase of delete columns, add nulls to incr data 
			var tempLandingDf: DataFrame = spark.emptyDataFrame
			var tempIncrDf: DataFrame = spark.emptyDataFrame
			if(!newCols.isEmpty) {
			  tempLandingDf = tempDf.select(getNewColumns(tempDf.columns.toSet, mergedCols):_*)
			}
			if(!delCols.isEmpty) {
			  tempIncrDf = incrDf.select(getNewColumns(incrDf.columns.toSet, mergedCols):_*)
			}
			
			//perform union of updated landing and incremental data frames
			var interDf: DataFrame = spark.emptyDataFrame
			if (!newCols.isEmpty && delCols.isEmpty) {
			  interDf = tempLandingDf.unionByName(incrDf)
			} else if(newCols.isEmpty && !delCols.isEmpty) {
			  interDf = tempDf.unionByName(tempIncrDf)
			} else if(newCols.isEmpty && delCols.isEmpty){
			  interDf = tempDf.unionByName(incrDf)
			}
			
			val finalCols = cols:::newCols
			
			df = interDf.select(finalCols.map(col): _*)
			
			df.cache()
			
			distinctCount = df.count()
			
			//make a backup of existing raw data
			var prefix = s3OutputPath.substring(0,s3OutputPath.lastIndexOf("/"))
			var dstnS3Path = prefix + "/backup/" + format.format(java.util.Calendar.getInstance().getTime())
			copyFiles(sc,s3OutputPath,dstnS3Path,false,true)
		  
			//sc.setCheckpointDir("s3://dif-checkpoint-data/checkpointData/" + format.format(Calendar.getInstance().getTime()))
			
			
		  //set the partition overwrite to dynamic
		  //this will help the replace only those partitions which are updated
			spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
		}
		else {
		  println("Performing a full load!!")
			//if query is null then table extract is performed
			if(queryForDataFetch.equalsIgnoreCase("null")) {
			  //if source key is null then it is a full table load
				if(sourcePrimaryKey.equalsIgnoreCase("null")) {
					df = spark.read.format("jdbc").option("driver", jdbcDriver).option("url", URL).option("user", username).option("password", password)
						.option("dbtable", rawObjectName).option("fetchsize", fetchSizeVal).option("inferSchema", "true").load()
					println("Fetched table from database using table extract!!")
				}
				//if the source key is not null then load is performed with the key (Will be faster)
				else {
					//fetch min and max of source primary key
					val pk_df = spark.read.format("jdbc").option("driver", jdbcDriver).option("url", URL).option("user", username).option("password", password)
							.option("query", s"select min($sourcePrimaryKey), max($sourcePrimaryKey), cast(count(*) as varchar(20)) as count from $rawObjectName").load()
					val min = pk_df.first()(0).toString.toDouble.toLong
					val max = pk_df.first()(1).toString.toDouble.toLong
					distinctCount = pk_df.first()(2).toString.toLong
					
					df = spark.read.format("jdbc").option("driver", jdbcDriver).option("url", URL).option("user", username).option("password", password)
						.option("dbtable", rawObjectName).option("fetchsize", fetchSizeVal).option("inferSchema", "true").option("lowerBound",min)
						.option("upperBound", max).option("numPartitions", noOfSourcePartitions).option("partitionColumn",sourcePrimaryKey).load()
					println("Fetched table from database using table extract along with partitioning!!")
				}
			}
			//if query is not null then query based extract is performed (will be performed on a single thread)
			else {
				df = spark.read.format("jdbc").option("driver", jdbcDriver).option("url", URL).option("user", username).option("password", password)
					.option("query", queryForDataFetch).option("fetchsize", fetchSizeVal).option("inferSchema", "true").load()
				println("Fetched table from database using query extract!!")
			}
			
			//do this to check if dataframe is empty
			if (df.rdd.isEmpty()) {
				println("Unable to retrieve the data from RDBMS!!")
				return false
			}
		}
		
		//By default every 1 Million rows will be partitioned
		noOfTargetPartitions = noOfTargetPartitions match {
		  case 0 => {
			  val partitionSizeDef: Int = 1000000
		    val maxPartitionSize: Int = 200
			  if(distinctCount < partitionSizeDef) 1
			  else {
			    if (maxPartitionSize < (distinctCount / partitionSizeDef).toInt) maxPartitionSize
			    else (distinctCount / partitionSizeDef).toInt
			  }
		  }
		  case a => a
		}
		
		spark.sql("CREATE DATABASE IF NOT EXISTS " + glueDatabase)
		
		if(targetPartitionColumn.equalsIgnoreCase("null")) {
		  //Create the glue table with the data without partition
		  df.coalesce(noOfTargetPartitions).write.option("path",s3OutputPath).mode("overwrite").saveAsTable(glueDatabase + "." + glueObjectName)
		  println("Table " + glueDatabase + "." + glueObjectName + " has been created at " + s3OutputPath + "!!")
		}
		else {
		  //Create the glue table with the data with partition
		  df.coalesce(noOfTargetPartitions).write.option("path",s3OutputPath).partitionBy(targetPartitionColumn).mode("overwrite").saveAsTable(glueDatabase + "." + glueObjectName)
		  spark.sql("MSCK REPAIR TABLE " + glueDatabase + "." + glueObjectName)
		  println("Table " + glueDatabase + "." + glueObjectName + " with partitions has been created at " + s3OutputPath)
		}
		
		if(!auditQuery.equalsIgnoreCase("null")) {
		  //create an object to validate source and target loads
		  val sourceDf = spark.read.format("jdbc").option("driver", jdbcDriver).option("url", URL).option("user", username).option("password", password)
					.option("query", auditQuery).option("inferSchema", "true").load()
		
		  val targetQuery = auditQuery.replace("'"+DBConnectionType.toLowerCase()+"'","'s3'").replaceAll(rawObjectName,glueDatabase+"."+glueObjectName).replaceAll("'"+rawObjectName.split("\\.")(0)+"'","'"+glueDatabase+"'")
		                  .replaceAll("'"+rawObjectName.split("\\.")(1)+"'","'"+glueObjectName+"'").replaceAll("varchar(100)","string")
		  val targetDf = spark.sql(targetQuery)
		
		  val auditDf = sourceDf.union(targetDf).withColumn("requestId",lit(requestId))
		
		  auditDf.write.option("path","s3://dif-raw-test/auditTable/").mode("append").saveAsTable(glueDatabase + ".meta_audit_table")
		  println("Audit Table " + glueDatabase + ".meta_audit_table has been created!!")
		}
		//df.unpersist()
		spark.stop()
		return true
	}
	
	def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
    merged_cols.toList.map(x => x match {
      case x if column.contains(x) => col(x)
      case _ => lit(null).as(x)
    })
  }
	
	def copyFiles(sc: SparkContext, srcS3Path: String, dstnS3Path: String, deleteSource : Boolean, overwrite : Boolean): Boolean = {
		//Source file system
		val srcPath: Path = new Path(srcS3Path.replace("s3:","s3a:"))
		val srcFs = FileSystem.get(srcPath.toUri, sc.hadoopConfiguration)
		
		//Destination file system
		val dstPath: Path = new Path(dstnS3Path.replace("s3:","s3a:"))
		val dstFs = FileSystem.get(dstPath.toUri, sc.hadoopConfiguration)
		
		//create destination folder if doesn't exists
		if (!dstFs.exists(dstPath)) dstFs.mkdirs(new Path(dstnS3Path))
		
		println("Moving raw folder from '" + srcS3Path + "' to '" + dstnS3Path + "'.")
		FileUtil.copy(srcFs, srcPath, dstFs, dstPath, deleteSource, overwrite, sc.hadoopConfiguration)
		true
	}
}