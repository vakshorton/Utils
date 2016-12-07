package com.hortonworks.util

object SparkPhoenixETL {
  import org.apache.spark._
  import org.apache.spark.SparkContext
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
    val transDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"TransactionHistory\"", "zkUrl" -> zkUrl))
    val itemTransDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"TransactionItems\"", "zkUrl" -> zkUrl))
    val productDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"Product\"", "zkUrl" -> zkUrl))
    
    import sqlContext.implicits._
    
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
  }
}