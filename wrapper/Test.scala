package main.scala

import java.sql.DriverManager
import java.sql.Connection
import scala.util.control.Breaks._
//import org.slf4j.Logger
//import org.slf4j.LoggerFactory
//import java.time.LocalDateTime
//import main.scala.Redirect

object Test {
  //val logger: Logger = LoggerFactory.getLogger(this.getClass())
  val hostname = "dif-metadata-newvpc.cwkzlbmbkvzg.us-east-1.rds.amazonaws.com"
  val url = "jdbc:postgresql://dif-metadata-newvpc.cwkzlbmbkvzg.us-east-1.rds.amazonaws.com:5432/postgres"
  val username = "postgres"
  val password = "postgres1234"
  val port = 5432
  val driver = "org.postgresql.Driver"
  
  //to store row wise data
  var rows = List[scala.collection.mutable.Map[String,Any]]()
  var requestId: Int = 0
  var isRunning = false
  
  def main(args: Array[String]) = {
    if(args.length != 1) throw new Exception("Pass Logical Group Id as an argument")
    val logicalGroupId = args(0)
    
    var oldRequest = false
    var statusFlag = 'S'
    var restartFlag = 'n'
    
    //Connecting to Database
    val connection: Connection = connectToDatabase(url,username,password,driver)
    
    if (connection == null) {
      println("Couldn't establish connection to Database!!")
      System.exit(1)
    }
    
    //Check if the logical group id is valid or not
    var tablename = "meta_logical_map"
    var query = "SELECT * FROM dif_metadata.meta_logical_map mlm INNER JOIN dif_metadata.meta_table_map mtm on mlm.logical_group_id=mtm.logical_group_id where mlm.logical_group_id = '" + logicalGroupId + "';"
    //println(query)
    if (!executeStatement(connection,query,tablename)) {
      connection.close()
      println("Please provide a valid logical group id!!")
      System.exit(1)
    }
    
    //check for running requests
    tablename = "meta_request"
    query = s"SELECT LOGICAL_GROUP_ID FROM dif_metadata.meta_request mr WHERE mr.logical_group_id='$logicalGroupId' AND mr.status_flag in ('P','S')"
    //println(query)
    if (!executeStatement(connection,query,tablename)) {
      println(s"Couldn't retrieve old requests for the logical group id : $logicalGroupId!!")
      connection.close()
      System.exit(1)
    }
    
    if(isRunning) {
      println("Job is already running!!")
      connection.close()
      System.exit(0)
    }
    
    //check for existing requests
    tablename = "meta_request"
    query = s"SELECT request_id FROM dif_metadata.meta_request mr WHERE mr.logical_group_id='$logicalGroupId' AND mr.status_flag='F' AND mr.restart_flag='true'"
    //println(query)
    if (!executeStatement(connection,query,tablename)) {
      println(s"Couldn't retrieve old requests for the logical group id : $logicalGroupId!!")
      connection.close()
      System.exit(1)
    }
    
    //break if there is an old request
    breakable {
      //if old request exists process it
      if(requestId != 0) {
        oldRequest = true
        println(s"Starting execution for request Id : $requestId!!")
        break
      }
      
      //Generate new request
      requestId = generateRequestId()
      println(s"Starting execution for request Id : $requestId!!")
      
      //Insert data into meta_request table
      tablename = "meta_request"
      query = "INSERT INTO dif_metadata.meta_request(request_id,logical_group_id,status_flag,restart_flag) VALUES(" + requestId + ", '" + logicalGroupId + "', '" + statusFlag + "', '" + restartFlag + "');"
      //println(query)
      if (!executeStatement(connection,query,tablename)) {
        println(s"Couldn't insert values in meta_request for Logical Group Id : $logicalGroupId!!")
        connection.close()
        System.exit(1)
      }
      
      //Insert data into meta_run_request table
      tablename = "meta_run_request"
      query = s"""INSERT INTO dif_metadata.meta_run_request
               |(request_id, logical_group_id, run_status_flag, run_priority, storage_layer, extract_type,
               |obj_schema, obj_name, transform_query, target_obj_schema, target_obj_name, obj_source, source_connection_type, source_secret_key,
               |obj_target, target_connection_type, target_secret_key, group_name, conformance_flip, pb_flip,
               |custom_flag, custom_query, delimiter_val, inc_header, for_archive, is_incremental,to_validate)
               |SELECT $requestId, '$logicalGroupId', '$statusFlag', mtm.run_priority, mtm.storage_layer, mtm.extract_type,
               |mtm.obj_schema, mtm.obj_name, mtm.transform_query, mtm.target_obj_schema, mtm.target_obj_name, mlm.obj_source,
               |mocm.connection_type, mocm.secret_key, mlm.obj_target, mocm1.connection_type, mocm1.secret_key, mlm.group_name,
               |mof.conformance_flip, mof.pb_flip, mtm.custom_flag, mcq.custom_query, mtm.delimiter_val, mtm.inc_header, mtm.for_archive, mtm.is_incremental,
               |mtm.to_validate FROM dif_metadata.meta_logical_map mlm INNER JOIN dif_metadata.meta_table_map mtm on mlm.logical_group_id=mtm.logical_group_id
               |INNER JOIN dif_metadata.meta_object_connection_map mocm on mlm.obj_source=mocm.obj_connection
               |INNER JOIN dif_metadata.meta_object_connection_map mocm1 on mlm.obj_target=mocm1.obj_connection
               |LEFT OUTER JOIN dif_metadata.meta_object_flip mof on mtm.target_obj_schema=mof.target_obj_schema and mtm.target_obj_name=mof.target_obj_name
               |LEFT OUTER JOIN dif_metadata.meta_custom_query mcq on mcq.logical_group_id='$logicalGroupId' and mtm.obj_schema=mcq.obj_schema
               |and mtm.obj_name=mcq.obj_name
               |WHERE mlm.logical_group_id='$logicalGroupId' and mtm.is_valid=true ORDER BY mtm.run_priority;""".stripMargin.replaceAll("\n"," ")
      //println(query)
      if (!executeStatement(connection,query,tablename)) {
        println(s"Couldn't insert values in meta_run_request for Logical Group Id : $logicalGroupId!!")
        connection.close()
        System.exit(1)
      }
    }
    
    //Update status in meta_request
    statusFlag = 'P'
    tablename = "meta_request"
    if(oldRequest) {
      query = s"""UPDATE dif_metadata.meta_request SET status_flag = '$statusFlag', end_date = null
              |where request_id=$requestId and logical_group_id='$logicalGroupId';""".stripMargin.replaceAll("\n"," ")
    }
    else {
      query = s"""UPDATE dif_metadata.meta_request SET status_flag = '$statusFlag', begin_date = now()
              |where request_id=$requestId and logical_group_id='$logicalGroupId';""".stripMargin.replaceAll("\n"," ")
    }
    //println(query)
    if (!executeStatement(connection,query,tablename)) {
      println(s"""Unable to update the status for request_id : $requestId!!, logical_group_id : $logicalGroupId""")
      connection.close()
      System.exit(1)
    }
    
    //Fetch job level details from meta_run_request
    tablename = "meta_run_request"
    query = s"""SELECT run_priority, storage_layer, extract_type, obj_schema, obj_name, transform_query, target_obj_schema, target_obj_name, 
            |obj_source, source_connection_type, source_secret_key, obj_target, target_connection_type, target_secret_key, group_name, 
            |conformance_flip, pb_flip, custom_flag, custom_query, delimiter_val, inc_header, for_archive, is_incremental, to_validate
            |FROM dif_metadata.meta_run_request mrr where mrr.request_id=$requestId and mrr.logical_group_id='$logicalGroupId' 
            |and mrr.run_status_flag<>'C' ORDER BY mrr.run_priority DESC;""".stripMargin.replaceAll("\n"," ")
    //println(query)
    if (!executeStatement(connection,query,tablename)) {
      println(s"""Unable to fetch details for request_id : $requestId!!, logical_group_id : $logicalGroupId""")
      connection.close()
      System.exit(1)
    }
    
    //Invoke Redirect class for each row fetched from meta_run_request
    for (i <- rows) {
      val runPriority = i("run_priority")
      val redirect: Redirect = new Redirect(connection,i,logicalGroupId,requestId)
      
      //Update status in meta_run_request before starting execution
      tablename = "meta_run_request"
      query = s"""UPDATE dif_metadata.meta_run_request SET run_status_flag = '$statusFlag', begin_date = now(), end_date = null
              |where request_id=$requestId and logical_group_id='$logicalGroupId' and run_priority=$runPriority;""".stripMargin.replaceAll("\n"," ")
      //println(query)
      breakable {
        if (!executeStatement(connection,query,tablename)) {
          println(s"""Unable to update the status for request_id : $requestId!!, 
                  |logical_group_id : $logicalGroupId and run priority : $runPriority""".stripMargin.replaceAll("\n"," "))
          break
        }
      
        var runStatusFlag = redirect.redirectFlow()
        if(!runStatusFlag) {
          println("Execution failed for Run Priority : " + runPriority)
          statusFlag = 'F'
          restartFlag = 'y'
        }
        else {
          println("Execution completed for Run Priority : " + runPriority)
          statusFlag = 'C'
        }
      
        //Update status in meta_run_request after execution
        tablename = "meta_run_request"
        query = s"""UPDATE dif_metadata.meta_run_request SET run_status_flag = '$statusFlag', end_date = now()
                |where request_id=$requestId and logical_group_id='$logicalGroupId' and run_priority=$runPriority;""".stripMargin.replaceAll("\n"," ")
        //println(query)
        if (!executeStatement(connection,query,tablename)) {
          println(s"""Unable to update the status for request_id : $requestId!!, 
                  |logical_group_id : $logicalGroupId and run priority : $runPriority""".stripMargin.replaceAll("\n"," "))
        }
      }
    }
    tablename = "meta_request"
    query = s"""UPDATE dif_metadata.meta_request SET status_flag = '$statusFlag',restart_flag='$restartFlag', end_date = now()
            |where request_id=$requestId and logical_group_id='$logicalGroupId';""".stripMargin.replaceAll("\n"," ")
    //println(query)
    if (!executeStatement(connection,query,tablename)) {
      println(s"""Unable to update the status for request_id : $requestId!!, logical_group_id : $logicalGroupId""")
      println(s"End execution for request Id : $requestId!!")
      connection.close()
      System.exit(1)
    }
    connection.close()
    println(s"End execution for request Id : $requestId!!")
  }
  
  //to generate Request ID
  def generateRequestId(): Int = {
    val rd = new scala.util.Random
    val rand = 100000000 + rd.nextInt(900000000)
    rand
  }
  
  //to connect to a database
  def connectToDatabase(url: String,username: String,password: String,driver: String): Connection = {
    var connection:Connection = null
    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    } catch {
        case e: Throwable => e.printStackTrace()
    }
    connection
  }
  
  //execute SQL queries on database
  def executeStatement(connection: Connection, query: String, tablename: String): Boolean = {
    // create the statement, and run the query
    val statement = connection.createStatement()
    
    //To check if Logical Group Id is valid
    if (tablename == "meta_logical_map") {
      try {
        val resultSet = statement.executeQuery(query)
        if (resultSet.next()) {
          //println("No. Of Rows Affected = " + statement.getUpdateCount())
          true
        } else {
          println("Logical Group Id is invalid")
          false
        }
      }
      catch {
        case e: Throwable => e.printStackTrace()
        false
      }
    }
    
    //Check for older requests
    else if ((tablename == "meta_request") && query.contains("SELECT")) {
      try {
        val resultSet = statement.executeQuery(query)
        val resultSetMetadata = resultSet.getMetaData()
        val columnCount = resultSetMetadata.getColumnCount()
        
        if(resultSet.next()){
          //will get string if there is already a job running other wise we get the request id
          resultSet.getObject(1).asInstanceOf[Any] match {
            case a:Long => requestId = a.asInstanceOf[java.lang.Long].toInt
            case a:String => isRunning = true
          }
        }
        //println(tablename + " : No.of rows fetched : " + rows.count(p=>true))
        true
      } catch {
        case e: Throwable => e.printStackTrace()
        false
      }
    }
    
    //Insert values into Meta Run Request and Meta Request
    else if ((tablename == "meta_request" || tablename == "meta_run_request") && query.contains("INSERT")) {
      try {
        statement.execute(query)
        //println(tablename + " : No. Of Rows Inserted = " + statement.getUpdateCount())
        true
      } catch {
        case e: Throwable => e.printStackTrace()
        false
      }
    }
    
    //Get values from Meta Run Request
    else if(tablename == "meta_run_request" && query.contains("SELECT")) {
      try {
        val resultSet = statement.executeQuery(query)
        val resultSetMetadata = resultSet.getMetaData()
        val columnCount = resultSetMetadata.getColumnCount()
        //Create map for each row fetched from meta_run_request and store those maps in a list
        while(resultSet.next()){
          var columnMapping = scala.collection.mutable.Map[String,Any]()
          for (i <- 1 to columnCount) {
            columnMapping += (resultSetMetadata.getColumnName(i) -> resultSet.getObject(i))
          }
          rows ::= columnMapping
        }
        //println(tablename + " : No.of rows fetched : " + rows.count(p=>true))
        true
      } catch {
        case e: Throwable => e.printStackTrace()
        false
      }
    }
    
    //Update status after execution
    else {
      try {
        statement.executeUpdate(query)
        //println(tablename + " : No. Of Rows Affected = " + statement.getUpdateCount())
        true
      } catch {
        case e: Throwable => e.printStackTrace()
        false
      }
    }
  }
}