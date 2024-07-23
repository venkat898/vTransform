package main.scala

import java.sql.DriverManager
import java.sql.Connection
import scala.sys.process._
import scala.util.parsing.json._
import scala.util.control.Breaks._
import java.io._

class Redirect(connection: Connection, map: scala.collection.mutable.Map[String,Any], logicalGroupId: String, requestId: Int) {
  
  def printVariables() {
    println("run_priority : " + map("run_priority")) 
    println("storage_layer : " + map("storage_layer")) 
    println("extract_type : " + map("extract_type")) 
    println("obj_schema : " + map("obj_schema")) 
    println("obj_name : " + map("obj_name")) 
    println("transform_query : " + map("transform_query")) 
    println("target_obj_schema : " + map("target_obj_schema")) 
    println("target_obj_name : " + map("target_obj_name")) 
    println("obj_source : " + map("obj_source")) 
    println("source_connection_type : " + map("source_connection_type")) 
    println("source_secret_key : " + map("source_secret_key")) 
    println("obj_target : " + map("obj_target")) 
    println("target_connection_type : " + map("target_connection_type")) 
    println("target_secret_key : " + map("target_secret_key")) 
    println("group_name : " + map("group_name")) 
    println("conformance_flip : " + map("conformance_flip")) 
    println("pb_flip : " + map("pb_flip")) 
    println("custom_flag : " + map("custom_flag")) 
    println("custom_query : " + map("custom_query")) 
    println("delimiter_val : " + map("delimiter_val")) 
    println("inc_header : " + map("inc_header")) 
    println("for_archive : " + map("for_archive")) 
    println("is_incremental : " + map("is_incremental")) 
    println("to_validate : " + map("to_validate")) 
  }
  
  //Redirect the flow
  def redirectFlow(): Boolean = {
    //Redirect based on the extract type variable
    map("storage_layer").toString() match {
      case "R" => rawIngestion()
      case "F" => fileIngestion()
      case "C" => confIngestion()
      case "P" => S3ToRedshift()
      case _ => {
        println("Invalid Storage layer!!")
        false
      }
    }
  }
  
  //raw ingestion to S3 from a database
  def rawIngestion(): Boolean = {
    //Build variables for raw ingestion
    val rawObjectName = map("obj_schema") + "." + map("obj_name")
    val s3Path = "s3://" + map("group_name") + "-raw-" + "dev" + "/" + map("obj_schema") + "/" + map("obj_name")
    val glueDB = map("group_name") + "_raw_" + "dev"
    val glueObjectName = map("obj_schema") + "__" + map("obj_name")
    
    //Print variables for raw ingestion
    println("Raw Ingestion for the object : " + rawObjectName)
    println("S3 Path : " + s3Path)
    println("Glue Object Name : " + glueDB + "." + glueObjectName)
    
    //fetch secret values using aws cli command
    var cmd = "aws secretsmanager get-secret-value --secret-id " + map("source_secret_key") + " --query SecretString"
    println("Executing command : " + cmd)
    val secretString = executeCommand(cmd)
    if(secretString == null) {
      println("Unable to fetch Secret Values")
      return false
    }
    //fetch the secret values from JSON format string
    val secretStringProcessed = secretString.stripPrefix("\"").stripSuffix("\"").trim.replaceAll("\\\\","")
    val secretMap = JSON.parseFull(secretStringProcessed).get.asInstanceOf[Map[String,Any]]
    
    val (dynamicQuery,auditQuery) = dynamicQueryBuild(secretMap)
    val transformQuery =map("extract_type").toString match {
      case "TB" => null
      case "QY" => {
        if(map("custom_flag").asInstanceOf[Boolean]) {
          map("custom_query") match {
            case null => {
              println("WARN: Custom flag is set to Y but there is no custom query. So using dynamic query!!")
              dynamicQuery
            }
            case a =>{
              println("Custom query is being used!!")
              a.toString
            }
          }
        }
        else dynamicQuery
      }
    }
    val validationQuery = if(map("to_validate").asInstanceOf[Boolean]) auditQuery else null
    
    //partition column extraction
    var query = s"""SELECT case source_partition_flag when true then source_primary_key else null end as source_primary_key,
                |case target_partition_flag when true then target_partition_column else null end as target_partition_column, source_partitions,
                |target_partitions FROM dif_metadata.meta_raw_properties where logical_group_id='$logicalGroupId'
                |and obj_schema='${map("obj_schema")}' and obj_name='${map("obj_name")}'""".stripMargin.replaceAll("\n"," ")
    var tablename = "meta_raw_properties"
    var partitionDetails = fetchDBDetails(query,tablename)
    
    if(partitionDetails.isEmpty) {
      partitionDetails = scala.collection.mutable.Map("source_primary_key" -> null, "target_partition_column" -> null, "source_partitions" -> 1, "target_partitions" -> 0)
    }
    
    //for incremental loads source primary key should be specified
    if(map("is_incremental").asInstanceOf[Boolean] && partitionDetails("source_primary_key") == null) {
      println("For all incremental loads source primary key must be specified!!")
      return false
    }
    
    //fetch cluster details from metadata for the current run priority
    query = s"""SELECT mtm.emr_group_id, emr_cluster_id, deploy_mode, executor_cores, executor_memory, driver_memory, no_of_executors
                |FROM dif_metadata.meta_emr_cluster_map mecm INNER JOIN dif_metadata.meta_table_map mtm on mtm.emr_group_id=mecm.emr_group_id
                |and mtm.logical_group_id='$logicalGroupId' and mtm.run_priority=${map("run_priority")}""".stripMargin.replaceAll("\n"," ")
    tablename = "meta_emr_cluster_map"
    val clusterDetails = fetchDBDetails(query,tablename)
    
    //fetch cluster IP address based on the cluster id
    cmd = "aws emr describe-cluster --cluster-id " + clusterDetails("emr_cluster_id") + " --query Cluster.MasterPublicDnsName"
    println("Executing command : " + cmd)
    val clusterDNS = executeCommand(cmd)
    if(clusterDNS == null) {
      println("Unable to fetch Cluster IP")
      return false
    }
    val clusterIP = clusterDNS.split("\\.")(0).replace("ip-","").replace("-",".").stripPrefix("\"").stripSuffix("\"").trim
    println("Submitting the job on : " + clusterIP)
    
    //fetch spark jar details from metadata
    query = "SELECT * FROM dif_metadata.meta_spark_jar_config"
    tablename = "meta_spark_jar_config"
    val jarDetails = fetchDBDetails(query,tablename)
    //println(jarDetails)
    
    //build command line for spark submit
    val sparkSubmit = s"""spark-submit
                      |--executor-cores ${clusterDetails("executor_cores")}
                      |--executor-memory ${clusterDetails("executor_memory")}
                      |--num-executors ${clusterDetails("no_of_executors")}
                      |--driver-memory ${clusterDetails("driver_memory")}
                      |--jars s3://${jarDetails("jar_bucket")}${jarDetails("jar_folder")}/postgresql-42.2.24.jar,s3://${jarDetails("jar_bucket")}${jarDetails("jar_folder")}/ojdbc8-19.3.0.0.jar
                      |--conf spark.sql.debug.maxToStringFields=1000
                      |--conf spark.io.compression.codec=snappy
                      |--conf mapreduce.fileoutputcommitter.algorithm.version=2
                      |--conf hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
                      |--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4
                      |--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
                      |--conf fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider
                      |--conf spark.dynamicAllocation.enabled=false
                      |--conf spark.shuffle.service.enabled=false
                      |--conf spark.rapids.sql.enabled=true
                      |--conf spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY
                      |--conf spark.sql.legacy.parquet.int96RebaseModeInRead=LEGACY
                      |--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY
                      |--conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY
                      |--class main.scala.S3RawIngestion
                      |s3://${jarDetails("jar_bucket")}${jarDetails("jar_folder")}/${jarDetails("jar_file")}
                      |"${secretMap("url")}" "${secretMap("username")}" "${secretMap("password")}" "$transformQuery" "$s3Path" "${map("for_archive")}"
                      |"$glueDB" "$rawObjectName" "$glueObjectName" "${map("source_connection_type")}" "${map("is_incremental")}"
                      |"${partitionDetails("source_primary_key")}" "${partitionDetails("target_partition_column")}"
                      |"${partitionDetails("source_partitions")}" "${partitionDetails("target_partitions")}" "$validationQuery" "$requestId"""".stripMargin.replaceAll("\n"," ")
		println("Executing Spark-Submit Command.... Please wait!!")
		println("Spark-Submit Command : " + sparkSubmit)
		
		//run the spark submit command and write the spark logs to a file
		val sparkState = executeSparkSubmit(sparkSubmit)
		
		//return false if there is any error in spark submit
		if(!sparkState) return false
		
		true
  }
  
  //Ingesting files (Json,csv,txt,tsv)
  def fileIngestion(): Boolean = {
    //Build variables for file ingestion
    val fileDirS3Path = "s3://" + map("group_name") + "-landing-" + "dev" + "/" + map("obj_schema")
    val glueDB = map("group_name") + "_raw_" + "dev"
    val glueObjectName = map("target_obj_schema") + "__" + map("target_obj_name")
    val fileType = map("extract_type") match {
                      case "JS" => "JSON"
                      case "CS" => "CSV"
                      case "TS" => "TSV"
                      case "TX" => "TXT"
                      case _ => null
                    }
    val delimiter = if (map("delimiter_val") != null) {
      map("extract_type") match {
         case "CS" => ","
         case "TS" => "\t"
      }
    } else {
      map("delimiter_val")
    }
    
    
    //Print variables for file ingestion
    println("File directory path : " + fileDirS3Path)
    println("File prefix : " + map("obj_name"))
    println("File type : " + fileType)
    println("Glue database : " + glueDB)
    println("Glue object name : " + glueObjectName)
    println("File header : " + map("inc_header"))
    println("Incremental : " + map("is_incremental"))
    println("delimiter : " + delimiter)
    
    //fetch spark jar details from metadata
    var query = "SELECT * FROM dif_metadata.meta_spark_jar_config"
    var tablename = "meta_spark_jar_config"
    val jarDetails = fetchDBDetails(query,tablename)
    //println(jarDetails)
    
    //fetch cluster details from metadata for the current run priority
    query = s"""SELECT mtm.emr_group_id, emr_cluster_id, deploy_mode, executor_cores, executor_memory, driver_memory, no_of_executors
                |FROM dif_metadata.meta_emr_cluster_map mecm INNER JOIN dif_metadata.meta_table_map mtm on mtm.emr_group_id=mecm.emr_group_id
                |and mtm.logical_group_id='$logicalGroupId' and mtm.run_priority=${map("run_priority")}""".stripMargin.replaceAll("\n"," ")
    tablename = "meta_emr_cluster_map"
    val clusterDetails = fetchDBDetails(query,tablename)
    
    //fetch cluster IP address based on the cluster id
    var cmd = "aws emr describe-cluster --cluster-id " + clusterDetails("emr_cluster_id") + " --query Cluster.MasterPublicDnsName"
    println("Executing command : " + cmd)
    val clusterDNS = executeCommand(cmd)
    if(clusterDNS == null) {
      println("Unable to fetch Cluster IP")
      return false
    }
    val clusterIP = clusterDNS.split("\\.")(0).replace("ip-","").replace("-",".").stripPrefix("\"").stripSuffix("\"").trim
    println("Submitting the job on : " + clusterIP)
    
    //build command line for spark submit
    val sparkSubmit = s"""spark-submit
                      |--executor-cores ${clusterDetails("executor_cores")}
                      |--executor-memory ${clusterDetails("executor_memory")}
                      |--num-executors ${clusterDetails("no_of_executors")}
                      |--driver-memory ${clusterDetails("driver_memory")}
                      |--conf spark.sql.debug.maxToStringFields=1000
                      |--conf spark.sql.caseSensitive=false
                      |--conf spark.sql.legacy.charVarcharAsString=true
                      |--conf mapreduce.fileoutputcommitter.algorithm.version=2
                      |--conf spark.io.compression.codec=snappy
                      |--conf hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
                      |--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4
                      |--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
                      |--conf fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider
                      |--conf spark.dynamicAllocation.enabled=false
                      |--conf spark.shuffle.service.enabled=false
                      |--conf spark.rapids.sql.enabled=true
                      |--conf spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY
                      |--conf spark.sql.legacy.parquet.int96RebaseModeInRead=LEGACY
                      |--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY
                      |--conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY
                      |--class main.scala.S3FileIngestion
                      |s3://${jarDetails("jar_bucket")}${jarDetails("jar_folder")}/${jarDetails("jar_file")}
                      |"$fileDirS3Path" "${map("obj_name")}" "$fileType" "$glueDB" "$glueObjectName" "${map("inc_header")}"
                      |"$delimiter" "${map("transform_query")}" "${map("is_incremental")}"""".stripMargin.replaceAll("\n"," ")
		println("Executing Spark-Submit Command : " + sparkSubmit)
		
		//run the spark submit command and write the spark logs to a file
		val sparkState = executeSparkSubmit(sparkSubmit)
		
		//return false if there is any error in spark submit
		if(!sparkState) return false
		
    true
  }
  
  //Conformance
  def confIngestion(): Boolean = {
    val confFlip = map("conformance_flip") match { 
      case "_a" => "_b" 
      case "_b" => "_a"
      case a => a
    }
    //Build variables for conformance
    val s3Path = "s3://" + map("group_name") + "-conformance-" + "dev" + "/" + map("target_obj_name") + confFlip
    val glueDB = map("group_name") + "_conformance_" + "dev"
    val glueObjectName = map("target_obj_name") + "_tv"
    
    //Print variables for conformance
    println("Conformance for the object : " + glueDB + "." + glueObjectName)
    println("S3 Path : " + s3Path)
    
    //fetch cluster details from metadata for the current run priority
    var query = s"""SELECT mtm.emr_group_id, emr_cluster_id, deploy_mode, executor_cores, executor_memory, driver_memory, no_of_executors
                |FROM dif_metadata.meta_emr_cluster_map mecm INNER JOIN dif_metadata.meta_table_map mtm on mtm.emr_group_id=mecm.emr_group_id
                |and mtm.logical_group_id='$logicalGroupId' and mtm.run_priority=${map("run_priority")}""".stripMargin.replaceAll("\n"," ")
    var tablename = "meta_emr_cluster_map"
    val clusterDetails = fetchDBDetails(query,tablename)
    
    //fetch cluster IP address based on the cluster id
    var cmd = "aws emr describe-cluster --cluster-id " + clusterDetails("emr_cluster_id") + " --query Cluster.MasterPublicDnsName"
    println("Executing command : " + cmd)
    val clusterDNS = executeCommand(cmd)
    if(clusterDNS == null) {
      println("Unable to fetch Cluster IP")
      return false
    }
    val clusterIP = clusterDNS.split("\\.")(0).replace("ip-","").replace("-",".").stripPrefix("\"").stripSuffix("\"").trim
    println("Submitting the job on : " + clusterIP)
    
    //fetch spark jar details from metadata
    query = "SELECT * FROM dif_metadata.meta_spark_jar_config"
    tablename = "meta_spark_jar_config"
    val jarDetails = fetchDBDetails(query,tablename)
    //println(jarDetails)
    
    //build command line for spark submit
    val sparkSubmit = s"""spark-submit
                      |--executor-cores ${clusterDetails("executor_cores")}
                      |--executor-memory ${clusterDetails("executor_memory")}
                      |--num-executors ${clusterDetails("no_of_executors")}
                      |--driver-memory ${clusterDetails("driver_memory")}
                      |--conf spark.sql.debug.maxToStringFields=1000
                      |--conf spark.io.compression.codec=snappy
                      |--conf mapreduce.fileoutputcommitter.algorithm.version=2
                      |--conf hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
                      |--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4
                      |--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
                      |--conf fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider
                      |--conf spark.dynamicAllocation.enabled=false
                      |--conf spark.shuffle.service.enabled=false
                      |--conf spark.rapids.sql.enabled=true
                      |--conf spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY
                      |--conf spark.sql.legacy.parquet.int96RebaseModeInRead=LEGACY
                      |--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY
                      |--conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY
                      |--class main.scala.S3Transformation
                      |s3://${jarDetails("jar_bucket")}${jarDetails("jar_folder")}/${jarDetails("jar_file")}
                      |"$s3Path" "$glueDB" "$glueObjectName" "${map("is_incremental")}" "${map("transform_query")}"""".stripMargin.replaceAll("\n"," ")
		println("Executing Spark-Submit Command : " + sparkSubmit)
		
		//run the spark submit command and write the spark logs to a file
		val sparkState = executeSparkSubmit(sparkSubmit)
		
		//return false if there is any error in spark submit
		if(!sparkState) return false
		
		//update flip after execution
		query = s"""Update dif_metadata.meta_object_flip set conformance_flip = '$confFlip' where target_obj_schema='${map("target_obj_schema")}'
                |and target_obj_name='${map("target_obj_name")}'""".stripMargin.replaceAll("\n","")
		tablename = "meta_object_flip"
		if(!Test.executeStatement(connection, query, tablename)){
		  println("Failure in updating meta_object_flip!!")
		  return false
		}
                
		true
  }
  
  //Move data from S3 to Redshift
  def S3ToRedshift(): Boolean = {
    val pbFlip = map("pb_flip") match { 
      case "_a" => "_b" 
      case "_b" => "_a"
      case a => a
    }
    //Build variables for Redshift
    val s3Path = "s3://" + map("group_name") + "-conformance-" + "dev" + "/" + map("obj_name") + map("conformance_flip") + "/"
    val redshiftSchema = map("group_name")
    val redshiftObjectName = map("target_obj_name") + pbFlip.toString
    val redshiftTVName = map("target_obj_name") + "_tv"
    
    //Print variables for redshift
    println("Redshift object : " + redshiftSchema + "." + redshiftObjectName)
    println("S3 Path : " + s3Path)
    
    //fetch secret values using aws cli command
    var cmd = "aws secretsmanager get-secret-value --secret-id " + map("target_secret_key") + " --query SecretString"
    println("Executing command : " + cmd)
    val secretString = executeCommand(cmd)
    if(secretString == null) {
      println("Unable to fetch Secret Values")
      return false
    }
    val secretStringProcessed = secretString.stripPrefix("\"").stripSuffix("\"").trim.replaceAll("\\\\","")
    val secretMap = JSON.parseFull(secretStringProcessed).get.asInstanceOf[Map[String,Any]]
    
    //create a redshift connection
    val driver = "org.postgresql.Driver"
    val redshiftConnection  = Test.connectToDatabase(secretMap("url").toString, secretMap("username").toString, secretMap("password").toString,driver)
    
    if (redshiftConnection == null) {
      println("Couldn't establish connection to Redshift!!")
      return false
    }
    
    //Build create, truncate and copy statements
    val createCommand = s"CREATE TABLE IF NOT EXISTS ${redshiftSchema}.${redshiftObjectName} ${map("transform_query")};"
    val truncateCommand = s"TRUNCATE TABLE ${redshiftSchema}.${redshiftObjectName};"
    val copyCommand = s"COPY ${redshiftSchema}.${redshiftObjectName} FROM '$s3Path' IAM_ROLE '${secretMap("iamRole")}' FORMAT AS PARQUET;"
    val createTVCommand = s"CREATE OR REPLACE VIEW ${redshiftSchema}.$redshiftTVName AS SELECT * FROM ${redshiftSchema}.${redshiftObjectName} WITH NO SCHEMA BINDING;"
    
    println("Creating table with Query : " + createCommand)
    if (!executeRedshiftQueries(redshiftConnection,createCommand)) return false
    println("Truncating table : " + truncateCommand)
    if (!executeRedshiftQueries(redshiftConnection,truncateCommand)) return false
    println("Copying data from s3 to redshift : " + copyCommand)
    if (!executeRedshiftQueries(redshiftConnection,copyCommand)) return false
    println("Data copied to Redshift!!")
    if (!executeRedshiftQueries(redshiftConnection,createTVCommand)) return false
    println("Technical view created!!")
    
    redshiftConnection.close()
    
    //update flip after execution
		var query = s"""Update dif_metadata.meta_object_flip set pb_flip = '$pbFlip' where target_obj_schema='${map("target_obj_schema")}'
                |and target_obj_name='${map("target_obj_name")}'""".stripMargin.replaceAll("\n","")
		var tablename = "meta_object_flip"
		if(!Test.executeStatement(connection, query, tablename)){
		  println("Failure in updating meta_object_flip!!")
		  return false
		}
    true
  }
  
  //execute spark submit commands
  def executeSparkSubmit(sparkSubmit: String): Boolean = {
    val sparkTuple = run(sparkSubmit)
		for (output <- sparkTuple._1) println("Spark Output : " + output)
		val applicationId = sparkTuple._1(0).split(" : ")(1)
		val directory = "/home/hadoop/logs/"
		val logFolder = new File(directory)
		if(!logFolder.exists()) {
		  logFolder.mkdirs()
		}
		val logFile = new File(directory + applicationId + ".log")
		val writer = new BufferedWriter(new FileWriter(logFile))
		for (output <- sparkTuple._2) writer.write(output + "\n")
		writer.close()
		if(sparkTuple._3 != 0) {
		  println("Process failed with exceptions. Please check log file " + directory + applicationId + ".log")
		  return false
		}
		true
  }
  
  //execute aws cli commands
  def executeCommand(cmd: String): String = {
    val rd = new scala.util.Random
    var counter = 5
    var exitCode = 5
    var stringOutput = ""
    var stringError = ""
    var sleepTimer = 0
    
    breakable {
    //Try to run the cli command.
    //if the command doesn't return the output, try to run the command for another 5 times until the correct output is returned
      while((exitCode!=0)&&(counter!=0)) {
        Thread.sleep(sleepTimer)
        val tuple = run(cmd)
        exitCode = tuple._3
        if(exitCode==999) {
          stringError = tuple._2(0)
          break
        }
        if(exitCode != 0) {
          sleepTimer = (5+rd.nextInt(6))*1000
          println("Couldn't fetch output for the command.. Retrying in " + sleepTimer/1000 + "seconds!!")
          stringError = tuple._2(0)
        }
        else {
          stringOutput = tuple._1(0)
        }
        counter -= 1
      }
    }
    
    //if the command doesn't return any output even after 5 tries, then return false
    if (exitCode!=0) {
      println(stringError)
      return null
    }
    return stringOutput
  }
  
  //runs shell commands and returns a tuple
  def run(in: String): (List[String], List[String], Int) = {
    val qb = Process(in)
    var out = List[String]()
    var err = List[String]()
    
    try {
      //store the result of the command in a tuple
      val exit = qb ! ProcessLogger((s) => out ::= s, (s) => err ::= s)
      return (out.reverse, err.reverse, exit)
    } catch {
      case e: Throwable => return (List(),List("Invalid Command!!"),999)
    }
  }
  
  //Returns the output of the query as a map
  def fetchDBDetails(query: String, tablename: String): scala.collection.mutable.Map[String,Any] = {
    var rows = List[scala.collection.mutable.Map[String,Any]]()
    val statement = connection.createStatement()
    var columnMapping = scala.collection.mutable.Map[String,Any]()
    try {
        val resultSet = statement.executeQuery(query)
        val resultSetMetadata = resultSet.getMetaData()
        val columnCount = resultSetMetadata.getColumnCount()
        
        //Create map for the row fetched from meta_emr_cluster_map
        if(resultSet.next()) {
          columnMapping = scala.collection.mutable.Map[String,Any]()
          for (i <- 1 to columnCount) {
            columnMapping += (resultSetMetadata.getColumnName(i) -> resultSet.getObject(i))
          }
        }
        
        //println(tablename + " : No.of rows fetched : " + rows.count(p=>true))
      } catch {
        case e: Throwable => e.printStackTrace()
      }
      columnMapping
  }
  
  //Run copy command
  def executeRedshiftQueries(conn:Connection, query: String): Boolean = {
    val statement = conn.createStatement()
    try {
      statement.execute(query)
      //println(tablename + " : No. Of Rows Inserted = " + statement.getUpdateCount())
      true
    } catch {
      case e: Throwable => e.printStackTrace()
      false
    }
  }

  //get the dynamic query
  def dynamicQueryBuild(secretMap:Map[String,Any]): (String,String) = {
    var jdbcDriver:String = ""
    var query: String = ""
    map("source_connection_type") match {
		  case "POSTGRES" => {
		    jdbcDriver = "org.postgresql.Driver"
		    query = ""
		    return (null,null)
		  }
		  case "ORACLE" => {
		    jdbcDriver = "oracle.jdbc.OracleDriver"
		    query = s"""SELECT
                    |'SELECT '||DBMS_XMLGEN.CONVERT(RTRIM(XMLAGG(XMLELEMENT(E,x.mySelectColumn,',').EXTRACT('//text()') ORDER BY X.COLUMN_ID).GetClobVal(),','),1)||' FROM '||X.OWNER||'.'||X.Object_NAME dbQuery,
                    |DBMS_XMLGEN.CONVERT(RTRIM(XMLAGG(XMLELEMENT(E,x.AuditColumn,' union ').EXTRACT('//text()') ORDER BY X.COLUMN_ID).GetClobVal(),' union '),1) auditQuery
                    |FROM
                    |(SELECT AO.OWNER,AO.Object_NAME,AC.COLUMN_ID,AC.DATA_TYPE,
                    |LOWER(CASE
                    |WHEN AC.DATA_TYPE = 'NUMBER' AND AC.DATA_PRECISION is null THEN 'CAST('||AC.COLUMN_NAME||' AS NUMBER(38,0)) AS '||AC.COLUMN_NAME
                    |WHEN AC.DATA_TYPE = 'ROWID' THEN 'ROWIDTOCHAR('||AC.COLUMN_NAME||') AS '||AC.COLUMN_NAME
                    |WHEN AC.DATA_TYPE = 'DATE' THEN
                    |'TO_DATE(case when substr(TO_CHAR('||AC.COLUMN_NAME||',''YYYY''),1,4) < ''1900'' THEN ''1900-01-01'' else TO_CHAR('||AC.COLUMN_NAME||', ''YYYY-MM-DD'') END, ''YYYY-MM-DD'') AS '|| lower(AC.COLUMN_NAME)
                    |WHEN AC.DATA_TYPE like 'TIMESTAMP%' THEN
                    |'TO_TIMESTAMP(case when substr(TO_CHAR('||AC.COLUMN_NAME||',''YYYY''),1,4) < ''1900'' THEN ''1900-01-01 00:00:00.000'' else TO_CHAR('||AC.COLUMN_NAME||', ''YYYY-MM-DD HH24:MI:SS.FF'') END, ''YYYY-MM-DD HH24:MI:SS.FF'') AS '|| lower(AC.COLUMN_NAME)
                    |ELSE AC.COLUMN_NAME
                    |END) as mySelectColumn,
                    |LOWER('SELECT ''ORACLE'' as source,'''||AO.OWNER||''' as schema_name ,'''||AO.Object_NAME||''' as table_name ,'''||AC.COLUMN_NAME||''' as column_name ,cast(MIN('||AC.COLUMN_NAME||') as varchar(100)) as min, cast(MAX('||AC.COLUMN_NAME||') as varchar(100)) as max, count(distinct '||AC.COLUMN_NAME||') as count from '||AO.OWNER||'.'||AO.Object_NAME) as AuditColumn
                    |FROM all_objects AO JOIN all_tab_columns AC
                    |ON AO.Owner = AC.Owner AND ao.object_type in ('TABLE','VIEW') AND AO.Object_NAME = AC.TABLE_NAME
                    |WHERE LOWER(AO.Object_NAME) = LOWER('${map("obj_name")}') AND LOWER(AO.OWNER) = LOWER('${map("obj_schema")}') ) X
                    |group by X.OWNER,X.Object_NAME""".stripMargin.replaceAll("\n"," ")
		  }
    }
    val sourceConnection  = Test.connectToDatabase(secretMap("url").toString, secretMap("username").toString, secretMap("password").toString,jdbcDriver)
    if(sourceConnection == null) {
      println("Couldn't establish connection to Source Database!!")
      System.exit(1)
    }
    
    try {
      val statement = sourceConnection.createStatement()
      val resultSet = statement.executeQuery(query)
      val resultSetMetadata = resultSet.getMetaData()
      val columnCount = resultSetMetadata.getColumnCount()
      //Create map for each row fetched from meta_run_request and store those maps in a list
      if(resultSet.next()){
        println("Dynamic SQL has been generated!!")
        val (str1,str2) = (resultSet.getObject(1).asInstanceOf[java.sql.Clob],resultSet.getObject(2).asInstanceOf[java.sql.Clob])
        return (clobToString(str1),clobToString(str2))
      }
    } 
    catch {
      case e: Throwable => e.printStackTrace()
    }
    sourceConnection.close()
    return (null,null)
  }
  
  //convert String to Clob
  def clobToString(clob:java.sql.Clob): String = {
    val br: BufferedReader = new BufferedReader(clob.getCharacterStream)
    val str = Stream.continually(br.readLine()).takeWhile(_ != null).mkString("\n")
    return str
  }
}