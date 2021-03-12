package com.spark.cos

import com.ibm.ibmos2spark.CloudObjectStorage
import org.apache.spark.sql.SparkSession
import com.ibm.stocator.fs.cos
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import java.util.Properties
import scala.io.Source
import scala.util.control.Exception._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_replace

object SparkIBM {

  val spark = SparkSession
    .builder()
    .appName("Connect IBM COS")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
  spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
  spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")

  def getProp(): Properties = {

    // reading the property file externally
    val url = getClass.getResource("application.properties")
    val prop: Properties = new Properties()
    if (url != null) {
      val source = Source.fromURL(url)
      prop.load(source.bufferedReader())
    } else {
      print("properties file cannot be loaded at path ")
    }
    return prop
  }

  def main(args: Array[String]) {

    val prop = getProp()

    val DB2_CONNECTION_URL = prop.getProperty("url")
    val endPoint = prop.getProperty("endPoint")
    val accessKey = prop.getProperty("accessKey")
    val secretKey = prop.getProperty("secretKey")
    val bucketName = prop.getProperty("accessKey")
    val objectname = prop.getProperty("accessKey")

    var credentials = scala.collection.mutable.HashMap[String, String](
      "endPoint" -> endPoint,
      "accessKey" -> accessKey,
      "secretKey" -> secretKey)

    var configurationName = "softlayer_cos"

    var cos = new CloudObjectStorage(spark.sparkContext, credentials, configurationName)

    var df = spark.
      read.format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load(cos.url(bucketName, objectname))

    Class.forName("com.ibm.db2.jcc.DB2Driver")
    prop.put("spark.sql.dialect", "sql")

    //writing the above read data into IBM DB2
    df.write.jdbc(DB2_CONNECTION_URL, "msc17587.emp_details", prop)

    //Reading back the table from DB2
    val df2 = spark.read.jdbc(DB2_CONNECTION_URL, "msc17587.emp_details", prop)

    df2.show()

    try {
      
      // replacing the special characters in Salary Column and casting to Integer 
      val df3 = df2.withColumn("Salary", regexp_replace(df2("Salary"), "[^0-9]", "").cast(IntegerType))

      
      // registering the table as a temporary table for sql operations
      df3.createOrReplaceTempView("emp")

      val ratio_df = spark.sql(""" select Department, 
    sum(case when Gender = 'Male' then 1 else 0 end)/count(*) male_ratio, 
    sum(case when Gender = 'Female' then 1 else 0 end)/count(*) female_ratio 
    from emp group by Department order by count(*) desc """)

      val avg_df = spark.sql(" select Department, avg(Salary) average_salary from emp group by Department ")

      val salary_gap_df = spark.sql(""" select Department, 
    round(avg(case when Gender='Male' then Salary end),0) avg_m_salary, 
    round(avg(case when Gender='Female' then Salary end),0) avg_f_salary, 
    (round(avg(case when Gender='Male' then Salary end),0) - round(avg(case when Gender='Female' then Salary end),0) ) diff_in_avg 
    from emp group by Department order by Department """)

      // writing the above genrated dataset into COS instance
    
  ratio_df.coalesce(1).write.format("parquet").save(cos.url(bucketName, "ratio.parquet"))
  avg_df.coalesce(1).write.format("parquet").save(cos.url(bucketName, "avg.parquet"))
  salary_gap_df.coalesce(1).write.format("parquet").save(cos.url(bucketName, "salary_gap_df.parquet"))
    } catch {
      case _: RuntimeException => println("caught a Runtime Exception")
      case _: Exception        => println("Unkown Exception")
    }

  }

}