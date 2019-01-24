package com.mutua.cmdigitalTransformation.alcance


import org.bson.Document
import org.apache.spark.sql._
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mutua.cmdigitalTransformation.alcance.Alcance_daily.{extractFBImpressions, transformFBImpressions}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, Interval}

import scala.util.{Failure, Success, Try}
import org.joda.time.format.DateTimeFormat


object Facebook_monthly {

  /**
    * Created by Francisco_Antiñolo_Faulkner on 26/06/2018.
    */


  def dateStringtoJoda(d: String): DateTime = {
    val yyyy = d.splitAt(4)._1
    val mm = d.splitAt(5)._2.splitAt(2)._1
    val dd = d.splitAt(5)._2.splitAt(3)._2.splitAt(2)._1
    //println(yyyy+mm+dd)

    val ret_date: DateTime = new DateTime(yyyy.toInt, mm.toInt, dd.toInt, 0, 0, 0, 0)
    ret_date
  }

  def transformFBImpressions(impressions: DataFrame, spark: SparkSession): DataFrame = {

    val impressions_RDD = impressions.rdd.map(t => {

      Alcance_RRSS_monthly(
        t.getAs[String]("end_time"),
        9,
        t.getAs[Int]("value"),
        t.getAs[Int]("value"),
        t.getAs[String]("name")
      )
    })

    val facebook_RDD = impressions_RDD.map(t => (
      dateStringtoJoda(t.id_date).getMonthOfYear,
      dateStringtoJoda(t.id_date).getYear,
      t.canal_e,
      t.impresionesP,
      t.impresionesO,
      t.name
    ))
    val facebook_RDD12 = facebook_RDD.map(t => (
      t._1,
      t._2,
      t._3,
      if (t._6 == "page_impressions") {
        t._4
      } else {
        0
      },
      if (t._6 == "page_impressions") {
        0
      } else {
        t._5
      },
      t._6

    ))



    val facebook_RDD2 = facebook_RDD12.map(t => (
      (t._1,
        t._2,
        t._3,
        t._6
      ), (t._1,
      t._2,
      t._3,
      t._4,
      t._5,
      t._6
    )))



    val facebook_RDD3 = facebook_RDD2.reduceByKey((x, y) => {
      (
        x._1,
        x._2,
        x._3,
        x._4 + y._4,
        x._5 + y._5,
        x._6
      )
    }).values




    val ret = spark.createDataFrame(facebook_RDD3);
    ret
  }

  def extractFBImpressions(spark: SparkSession, config: Map[String, String]): DataFrame = {

    import spark.implicits._
    // EJECUCION LOCAL
    //  val readConfig = ReadConfig(Map("uri" -> mongoUri, "collection" -> "facebook"))
    val readConfig = ReadConfig(Map("uri" -> config("MONGOURI"), "database" -> config("FACEBOOKDB"), "collection" -> config("FACEBOOKCOLLECTION")))
    val df = MongoSpark.load(spark, readConfig)

    //NOTA: Utilizar sparkContext devuelve un RDD, si se pasa como parámetro sparkSession, devuelve un DataFrame

    println(df.count)
    df.show()

    val prueba = df.select(explode(col(colName = "data")).as("data"))
    prueba.show

    prueba.printSchema

    val prueba2 = prueba.select("data.*").show


    val d = prueba.filter($"data.period" === "day")
    val day = d.select("data.*")


    val value = day.select(col("name"), explode(col(colName = "values")).as("values"))

    val impressions = value.filter(col("values.end_time").rlike("2018-05")).select("name", "values.value", "values.end_time")

    impressions
  }


  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val mongoUri = config.getString("MONGO_URI")
    val database = config.getString("FACEBOOK_DB")
    val collection = config.getString("FACEBOOK_COLLECTION")

    val conf = Map(
      "MONGOURI" -> mongoUri,
      "FACEBOOKDB" -> database,
      "FACEBOOKCOLLECTION" -> collection
    )


    val spark = SparkSession.builder().master("local").appName("MongoTest").getOrCreate()


    val impressions = extractFBImpressions(spark, conf);
    impressions.toDF().show


    val finalDF = transformFBImpressions(impressions, spark)
    finalDF.show(100)



  }

}