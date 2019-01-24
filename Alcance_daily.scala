package com.mutua.cmdigitalTransformation.alcance

import org.apache.spark.sql._
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, Interval}

import org.joda.time.format.DateTimeFormat

object Alcance_daily {

  /**
    * Created by Francisco_Antiñolo_Faulkner on 27/06/2018.
    */

  def dateStringtoJoda(d: String): DateTime = {
    val yyyy = d.splitAt(4)._1
    val mm = d.splitAt(5)._2.splitAt(2)._1
    val dd = d.splitAt(5)._2.splitAt(3)._2.splitAt(2)._1
    //println(yyyy+mm+dd)

    val ret_date: DateTime = new DateTime(yyyy.toInt, mm.toInt, dd.toInt, 0, 0, 0, 0)
    ret_date
  }


  // Función que lee los datos de las impresiones de MongoDB, los filtra para obtener sólamente los del mes pedido y los devuelve en un DF.

  def extractFBImpressions(spark: SparkSession, config: Map[String, String]): DataFrame = {

    import spark.implicits._

    // Lectura de de datos desde MongoDB y guardados en un DF.
    //val readConfig = ReadConfig(Map("uri" -> config("MONGO_TRANSFORMATION") ,"collection" -> "facebook"))
    // LOCAL
    val readConfig = ReadConfig(Map("uri" ->"mongodb://127.0.0.1:27017" ,"database" -> "base", "collection" -> "facebook"))
    val df = MongoSpark.load(spark, readConfig)

    //NOTA: Utilizar sparkContext devuelve un RDD, si se pasa como parámetro sparkSession, devuelve un DataFrame

    val prueba = df.select(explode(col(colName = "data")).as("data"))

    // Se guarda en una variable las fechas de inicio y final del mapa config
    val starDate = config("FACEBOOK_START_DATE")
    val endDate = config("FACEBOOK_END_DATE")



    val prueba2 = prueba.select("data.*")


    val d = prueba.filter($"data.period" === "day")
    val day = d.select("data.*")


    val value = day.select(col("name"), explode(col(colName = "values")).as("values"))


    // Se realiza un filtro para obtener los días del mes pedido por parámetro. También se obtiene el día 1 del siguiente mes y se desecha el día 1 del propio mes
    // ya que las impresiones muestran los datos del día anterior.

    val impressions = value.filter(col("values.end_time").rlike(starDate.toString.substring(0,7)) || col(colName = "values.end_time").rlike(endDate.toString.substring(0,7).concat("-01"))).select("name", "values.value", "values.end_time")
    val impressions2 = impressions.filter(!col("values.end_time").rlike(starDate.toString.substring(0,7).concat("-01")))

    impressions2
  }

  // Función que transforma los datos de las impresiones del mes al formato correcto para poder introducirlos en las tablas de hechos de PostgreSQL

  def transformFBImpressions(df : DataFrame, spark :SparkSession, config: Map[String, String] ): DataFrame = {

    val id_load_date = config("ID_LOAD_DATE")



    val impressions_RDD = df.rdd.map(t => {

      Alcance_RRSS_daily(
        t.getAs[String]("end_time").substring(0,10),
        9,
        t.getAs[Int]("value"),
        t.getAs[Int]("value"),
        t.getAs[String]("name"),
        id_load_date.toLong

      )
    })

    val facebook_RDD = impressions_RDD.map(t => (
      dateStringtoJoda(t.id_date).minusDays(1).toString.substring(0,10) ,
      t.canal_e,
      t.organic,
      t.paid,
      t.name,
      id_load_date
    ))


    val facebook_RDD2 = facebook_RDD.map(t => (
      (t._1 ,
        t._2,
        t._3,
        t._4,
        t._5,
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
        x._4,
        x._5,
        x._6
      )
    }).values



    // Se introducen cada uno de los tipos de impresiones en la columna correspondiente.

    val facebook_RDD4 = facebook_RDD3.map(t => (
      t._1,
      t._2,
      if (t._5 == "page_impressions_paid") {
        0
      } else {
        t._3
      },
      if (t._5 == "page_impressions_paid") {
        t._4
      } else {
        0
      },
      t._5,
      t._6

    ))


   // Se mapea para realizar el reduce
    val facebook_RDD5 = facebook_RDD4.map(t => (
      (t._1,
        t._2,
        t._6
      ), (t._1,
      t._2,
      t._3,
      t._4,
      t._6


    )))


    // Se suman las columnas de impresiones con el mismo día para no tenerlo con cero.

    val facebook_RDD6 = facebook_RDD5.reduceByKey((x, y) => {
      (
        x._1,
        x._2,
        x._3 + y._3,
        x._4 + y._4,
        x._5
      )
    }).values




    val ret = spark.createDataFrame(facebook_RDD6).withColumnRenamed("_1", "id_date").withColumnRenamed("_2", "canal_e")
      .withColumnRenamed("_3", "organico").withColumnRenamed("_4", "pagado").withColumnRenamed("_5", "id_load_date")
      .withColumn("id_load_date", to_utc_timestamp(from_unixtime(unix_timestamp()), "ES"))

    ret.rdd.take(50).foreach(println)



    ret
  }


    def main(args: Array[String]): Unit = {

    val id_load_date = (System.currentTimeMillis / 1000).toString

    val config = ConfigFactory.load()
    val database = config.getString("FACEBOOK_DB")
    val collection = config.getString("FACEBOOK_COLLECTION")

      val mongoUri = "mongodb://" + config.getString("MONGODB_USER")+
        ":" + config.getString("MONGODB_PASS")+"@" + config.getString("MONGODB_URL")+"/" +
        config.getString("MONGODB_BD")+"?authSource=" + config.getString("MONGODB_AUTH")

      // Se calculan las fechas a partir del parámetro de entrada.

      val date = args(0)
      val start_date: DateTime = new DateTime(date.substring(0,4).toInt, date.substring(4).toInt, 1, 0, 0, 0, 0).plusDays(1)
      val end_date = start_date.dayOfMonth.withMaximumValue.plusDays(1)
      val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")

      // Mapa de configuraciones

      val conf = Map(
        "MONGO_TRANSFORMATION" -> mongoUri,
        "FACEBOOKDB" -> database,
        "FACEBOOKCOLLECTION" -> collection,
        "FACEBOOK_START_DATE" -> fmt.print(start_date),
        "ID_LOAD_DATE" -> id_load_date,
        "FACEBOOK_END_DATE" -> fmt.print(end_date),
        "FACEBOOOK_TIMESPAN" -> "RANGEDATE"

      )


    val spark = SparkSession.builder().master("local").appName("MongoTest").getOrCreate()



    val impressions =  extractFBImpressions(spark, conf);


    val finalDF = transformFBImpressions(impressions, spark, conf)

  }

}