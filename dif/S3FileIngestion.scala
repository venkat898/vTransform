package main.scala

import org.apache.spark.sql._
import scala.collection.mutable._
import org.apache.hadoop.fs._
import java.net.URI
import org.apache.spark.SparkContext
import scala.util.control.Breaks._
import org.apache.spark.sql.functions.col

object S3FileIngestion {
	var spark: SparkSession = _
	
	//declaring console arguments globally
	var fileLandingPath: String = ""
	var filePrefix: String = ""
	var fileType: String = ""
	var glueDatabase: String = ""
	var glueObjectName: String = ""
	var incHeader: String = ""
	var delimiter: String = ""
	var fileSchema: String = ""
	var fileSelect: String = ""
	var isIncremental: String = ""
	
	def main(args: Array[String]): Unit = {
  spark = SparkSession
					.builder()
					.appName("S3 File Ingestion")
					.enableHiveSupport()
					.getOrCreate()
    println("Application Id : " + spark.sparkContext.applicationId)
	  if(fileIngestion(args)) {
			println("Process completed succesfully!!")
		}
		else {
			println("Process failed!!")
			throw new Exception("Process failed!!")
		}
  }
	
	//Redirect Flow to appropriate file ingest method
	def fileIngestion(consoleArgs: Array[String]): Boolean = {
	  
	  if ( consoleArgs.length.toInt != 9) throw new Exception("Please pass (9) arguments. Current No. of arguments passed : " + consoleArgs.length.toInt)
	  
	  fileLandingPath = consoleArgs(0).toString
	  filePrefix = consoleArgs(1).toString
	  fileType = consoleArgs(2).toString
	  glueDatabase = consoleArgs(3).toString
	  glueObjectName = consoleArgs(4).toString
	  incHeader = consoleArgs(5).toString
	  delimiter = consoleArgs(6).toString
	  fileSchema = consoleArgs(7).toString.split("::::")(0)
	  fileSelect = consoleArgs(7).toString.split("::::")(1)
		isIncremental = consoleArgs(8).toString
	  
	  fileType match {
	    case "JSON" => jsonFileIngestion()
	    case "CSV" | "TSV" | "TXT" => delimetedFileIngestion()
	    case _ => {
	      println("Invalid file type!!")
	      return false
	    }
	  }
	}
	
	//Json File Ingest
	def jsonFileIngestion(): Boolean = {
	  
	  //spark.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)
	  //get s3 paths for staging and processed folders
	  val fileStagingPath = fileLandingPath.replaceAll("/landing/","/staging/")
	  val fileProcessedPath = fileLandingPath.replaceAll("/landing/","/processed/")
	  
	  //extract bucket name and create raw bucket name
	  val regex = "(s3://[a-zA-Z-0-9]*).*".r
	  val fileRawBucket = fileLandingPath match { case regex(s) => s.replaceAll("-landing-","-raw-") case _ => "NA"}
	  if(fileRawBucket.equalsIgnoreCase("NA")) {
	    println("Cannot build raw bucket. Landing path should be of format 's3://<bucket>/<sub-directories>/'.")
	    return false
	  }
	  val fileRawPath = fileRawBucket  + "/" + glueObjectName.split("__")(0) + "/"+glueObjectName.split("__")(1)+"/"
	  
	  var toOverwrite = if (isIncremental.equalsIgnoreCase("false")) "overwrite" else "append"
	  
	  //set hadoop config for moving files
	  val sc = spark.sparkContext
		
		//move files from one folder to other
		if(!moveFiles(sc,fileLandingPath,fileStagingPath,true,true)) return true
	  
		//read s3 directory for json files
	  val fileDF = spark.read.option("multiLine", "true").option("inferTimestamp","false").json(fileStagingPath)
	  //.withColumn("fileName", callUDF("get_file_name", input_file_name()))
	  fileDF.cache()
	  
	  val cols = fileDF.columns.toList
	  val flatTuple = flatten(fileDF,cols)
	  val df = flatTuple._1
	  
	  df.write.option("path",fileRawPath).mode(toOverwrite).saveAsTable(glueDatabase + "." + glueObjectName)
	  
    moveFiles(sc,fileStagingPath,fileProcessedPath,true,true)
    
	  true
	}
	
	//Delimeted file Ingestion
	def delimetedFileIngestion(): Boolean = {
	  
	  //spark.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)
	  //get s3 paths for staging and processed folders
	  val fileStagingPath = fileLandingPath.replaceAll("/landing/","/staging/")
	  val fileProcessedPath = fileLandingPath.replaceAll("/landing/","/processed/")
	  
	  //extract bucket name and create raw bucket name
	  val regex = "(s3://[a-zA-Z-0-9]*).*".r
	  val fileRawBucket = fileLandingPath match { case regex(s) => s.replaceAll("-landing-","-raw-") case _ => "NA"}
	  if(fileRawBucket.equalsIgnoreCase("NA")) {
	    println("Cannot build raw bucket. Landing path should be of format 's3://<bucket>/<sub-directories>/'.")
	    return false
	  }
	  val fileRawPath = fileRawBucket  + "/" + glueObjectName.split("__")(0) + "/"+glueObjectName.split("__")(1)+"/"
	  
	  var toOverwrite = if (isIncremental.equalsIgnoreCase("false")) "overwrite" else "append"
	  
	  //set hadoop config for moving files
	  val sc = spark.sparkContext
		
		//move files from one folder to other
		if(!moveFiles(sc,fileLandingPath,fileStagingPath,true,true)) return true
	  
		//read s3 directory for json files
	  val fileDF = spark.read.option("delimiter",delimiter).option("header",incHeader).option("inferSchema","true").csv(fileStagingPath)
	  //.withColumn("fileName", callUDF("get_file_name", input_file_name()))
	  
	  fileDF.write.option("path",fileRawPath).mode(toOverwrite).saveAsTable(glueDatabase + "." + glueObjectName)
	  
    moveFiles(sc,fileStagingPath,fileProcessedPath,true,true)
    
	  true
	}
	
	//Flatten the dataframe
	def flatten(df: DataFrame,cols: List[String]):(DataFrame, List[String]) = {
    var isStruct = false
    var flatCols = List[String]()
    var nestedCols = List[String]()
    //check if there are any nested columns
    breakable {
        for (field <- df.schema.fields) {
            if (field.dataType.getClass.getSimpleName == "StructType") {
                isStruct = true
                break
            }
        }
    }
    //if nested column exists, flatten it. Otherwise return the dataframe
    if (isStruct) {
        flatCols = for(field <- df.schema.fields.toList if (field.dataType.getClass.getSimpleName != "StructType")) yield field.name
        nestedCols = for(field <- df.schema.fields.toList if (field.dataType.getClass.getSimpleName == "StructType")) yield field.name
        var cols = flatCols ++ nestedCols.map(_+".*")
        var flatDf = df.select(cols.map(col): _*)
        return flatten(flatDf,cols)
    } else {
        return (df,cols)
    }
  }
	
	//move files from one bucket to another
	def moveFiles(sc: SparkContext, srcS3Path: String, dstnS3Path: String, deleteSource : Boolean, overwrite : Boolean): Boolean = {
	  //Source file system
	  val srcPath: Path = new Path(srcS3Path)
    val srcFs = FileSystem.get(srcPath.toUri, sc.hadoopConfiguration)
    
    //Destination file system
    val dstPath: Path = new Path(dstnS3Path)
    val dstFs = FileSystem.get(dstPath.toUri, sc.hadoopConfiguration)
    
    //create destination folder if doesn't exists
    if (!dstFs.exists(dstPath)) dstFs.mkdirs(new Path(dstnS3Path))
    
    //Get all file paths from source path
    val paths: ListBuffer[Path] = ListBuffer()
    val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = srcFs.listFiles(new Path(srcS3Path), true)
    while (fileStatusListIterator.hasNext) {
      val fileStatus: LocatedFileStatus = fileStatusListIterator.next()
      paths += fileStatus.getPath
    }
	  
	  //if landing path is empty exit from job
	  if(srcS3Path.contains("/landing/")) {
	    if(paths.isEmpty) {
	      println("No files to process in landing path!!")
	      return false
	    }
	  }
	  
	  println("Moving files from '" + srcS3Path + "' to '" + dstnS3Path + "'.")

	  var validFileCounter = 0
	  //copy all files to destination and delete the files in source after copy
    paths.foreach { path =>
      val fileName = path.toUri.getRawPath.split("/").last
      val finalPath = dstnS3Path + fileName
      if (finalPath.matches(".*(("+filePrefix+").*("+fileType.toLowerCase()+"$))")) {
        FileUtil.copy(srcFs, path, dstFs, new Path(finalPath), deleteSource, overwrite, sc.hadoopConfiguration)
        validFileCounter += 1
      }
	  }
	  if(srcS3Path.contains("/landing/")) {
	    if(validFileCounter == 0) {
	      println("No files to process in landing path!!")
	      return false
	    }
	  }
	  true
  }
}