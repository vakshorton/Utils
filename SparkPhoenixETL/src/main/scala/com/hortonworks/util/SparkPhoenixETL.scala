package com.hortonworks.util

object SparkPhoenixETL {
  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SQLContext
  import org.apache.phoenix.spark._ 
  import org.apache.spark.sql.hive._ 
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Phoenix ETL")
      //.config("spark.some.config.option", "some-value")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val zkUrl = args(0) //"zk_host:zk_port:zk_path"
    val sourceClusterName = args(1)
    val applicationType = args(2)
    
    import sqlContext.implicits._
    
    applicationType match {
      case "DeviceManager" =>
        val deviceLogDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"DeviceStatusLog\"", "zkUrl" -> zkUrl))
        val deviceDetailDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"DeviceDetailsBI\"", "zkUrl" -> zkUrl))
        
        deviceLogDF.registerTempTable("phoenix_device_status_log")
        deviceDetailDF.registerTempTable("phoenix_device_details")
        
        //sqlContext.sql("CREATE TABLE IF NOT EXISTS telecom_device_status_log_"+sourceClusterName+" (serialNumber string, status string, state string, internalTemp int, signalStrength int, eventTimeStamp bigint) CLUSTERED BY (serialNumber) INTO 30 BUCKETS STORED AS ORC")
        //sqlContext.sql("CREATE TABLE IF NOT EXISTS telecom_device_details_"+sourceClusterName+" (serialNumber string, deviceModel string, latitude string, longitude string, ipAddress string, port string) CLUSTERED BY (serialNumber) INTO 30 BUCKETS STORED AS ORC")
        
        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        sqlContext.sql("insert into table telecom_device_status_log_"+sourceClusterName+" select serialNumber, status, state, internalTemp, signalStrength, eventTimeStamp from phoenix_device_status_log")
        sqlContext.sql("insert into table telecom_device_details_"+sourceClusterName+" select serialNumber, deviceModel, latitude, longitude, ipAddress, port from phoenix_device_details")
      case "CreditFraud" => 
        val transDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"TransactionHistory\"", "zkUrl" -> zkUrl))
        //val customerDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"CustomerAccount\"", "zkUrl" -> zkUrl))
        
        transDF.registerTempTable("phoenix_transaction_history")
        //customerDF.registerTempTable("phoenix_customer_account")

        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        sqlContext.sql("insert into table transaction_history_"+sourceClusterName+" partition(accounttype) select accountnumber,frauduent,merchantid,merchanttype,amount,currency,iscardpresent,latitude,longitude,transactionid,transactiontimestamp,distancefromhome,distancefromprev from phoenix_transaction_history")
        //sqlContext.sql("insert into table customer_account_"+sourceClusterName+" ")
      case "Retail" =>
        val transDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"TransactionHistory\"", "zkUrl" -> zkUrl))
        val itemTransDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"TransactionItems\"", "zkUrl" -> zkUrl))
        val productDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"Product\"", "zkUrl" -> zkUrl))
        
        transDF.registerTempTable("phoenix_retail_transactions")
        itemTransDF.registerTempTable("phoenix_retail_item_transactions")
        productDF.registerTempTable("phoenix_retail_products")

        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
  
        //transDF.write.format("orc").save("retail_transaction_history")
        //transDF.write.format("orc").partition("accountType","shipToState"),save("retail_transaction_history")
        //transDF.write.format("orc").partition("accountType","shipToState"),insertInto("select transactionId,locationId, item, accountNumber,amount,currency,isCardPresent,transactionTimeStamp,accountType,shipToState from phoenix_retail_transactions")

        //sqlContext.sql("CREATE TABLE IF NOT EXISTS retail_transaction_history (transactionId String,locationId String, item String, accountNumber String, amount Double, currency String, isCardPresent String, ipAddress String, transactionTimeStamp String) PARTITIONED BY (accountType String, shipToState String) CLUSTERED BY (accountNumber) INTO 30 BUCKETS STORED AS ORC")
        //sqlContext.sql("CREATE TABLE IF NOT EXISTS retail_products (productId String, productCategory String, manufacturer String, productName String, price Double) PARTITIONED BY (productSubCategory String) CLUSTERED BY (manufacturer) INTO 30 BUCKETS STORED AS ORC")
        sqlContext.sql("insert into table retail_transaction_history_"+sourceClusterName+" partition(accountType,shipToState) select a.transactionId, locationId, productId as item, accountNumber,amount,currency,isCardPresent,transactionTimeStamp,accountType,shipToState from phoenix_retail_transactions as a, phoenix_retail_item_transactions as b where a.transactionId = b.transactionId")
        sqlContext.sql("insert into table retail_products_"+sourceClusterName+" partition(productSubCategory) select productId, productCategory,  manufacturer, productName, price, productSubCategory from phoenix_retail_products")
      case _ => println("Unknown Application Type... Exit")
    } 
  }
}