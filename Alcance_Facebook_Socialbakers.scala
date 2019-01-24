import java.util.Properties

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mutua.cmdigitalTransformation.alcance.{Alcance_FB_SocialBakers, Alcance_RRSS_daily}
import com.mutua.cmdigitalTransformation.alcance.Alcance_daily.dateStringtoJoda
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by Francisco_Antiñolo_Faulkner on 23/08/2018.
  */

object Alcance_Facebook_Socialbakers {


  def dateStringtoJoda(d: String): DateTime = {
    val yyyy = d.splitAt(4)._1
    val mm = d.splitAt(5)._2.splitAt(2)._1
    val dd = d.splitAt(5)._2.splitAt(3)._2.splitAt(2)._1
    //println(yyyy+mm+dd)

    val ret_date: DateTime = new DateTime(yyyy.toInt, mm.toInt, dd.toInt, 0, 0, 0, 0)
    ret_date
  }


  def extractFBImpressions(spark: SparkSession, conf: Map[String, String]): DataFrame = {
    import spark.implicits._

    // Lectura de de datos desde MongoDB y guardados en un DF.
    //val readConfig = ReadConfig(Map("uri" -> config("MONGO_TRANSFORMATION") ,"collection" -> "facebook"))
    // LOCAL
    val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1:27017", "database" -> "base", "collection" -> "yury"))
    val df = MongoSpark.load(spark, readConfig)

    val mongoUri = "mongodb://eos.cmd.batch:Mutua18@iaws-mongodb1-eos-prod.mutua.es:27017,iaws-mongodb2-eos-prod.mutua.es:27017,iaws-mongodb3-eos-prod.mutua.es:30000/cmd?authSource=admin"
  //  val readConfig = ReadConfig(Map("uri" -> mongoUri, "collection" -> "socialbakers_header"))
  //  val df = MongoSpark.load(spark, readConfig)
    //NOTA: Utilizar sparkContext devuelve un RDD, si se pasa como parámetro sparkSession, devuelve un DataFrame
    df.show()

    val prof = df.select(explode(col(colName = "profiles")).as("profiles"))

    prof.show(1, false)

    val prof2 = prof.select("profiles.*")


    // Se guarda en una variable las fechas de inicio y final del mapa config
    val starDate = conf("FACEBOOK_START_DATE")
    val endDate = conf("FACEBOOK_END_DATE")

    val prof3 = prof2.select(explode(col(colName = "data")).as("data"))
    val proff = prof3.select("data.*").show(500)

    val prof5 = prof3.filter(col("data.date").rlike(starDate.toString.substring(0, 7)) || col(colName = "data.date").rlike(endDate.toString.substring(0, 7))).select("data.date", "data.insights_impressions_organic", "data.insights_impressions_paid")
    val prof6 = prof5.filter(!col("data.insights_impressions_organic").rlike("null") && (!col("data.insights_impressions_paid").rlike("null")))

    prof6
    //  val prof4 = prof6.select("data.date","data.insights_impressions_organic", "data.insights_impressions_paid").show(90)


  }

  def transformFBImpressions(df: DataFrame, spark: SparkSession, config: Map[String, String]): Dataset[Row] = {

    val id_load_date = config("ID_LOAD_DATE")


    val impressions_RDD = df.rdd.map(t => {

      Alcance_FB_SocialBakers(
        t.getAs[String]("date").substring(0, 10),
        9,
        t.getAs[Int]("insights_impressions_organic"),
        t.getAs[Int]("insights_impressions_paid"),
        id_load_date.toLong

      )
    })

    impressions_RDD.foreach(println)


    val facebook_RDD = impressions_RDD.map(t => (
      dateStringtoJoda(t.id_date).toString.substring(0, 10),
      t.canal_e,
      t.impresionesO,
      t.impresionesP,
      id_load_date
    ))

    val facebook_RDD2 = facebook_RDD.map(t => (
      (t._1 ,
        t._2,
        t._3,
        t._4,
        t._5
      ), (t._1,
      t._2,
      t._3,
      t._4,
      t._5
    )))


    val facebook_RDD3 = facebook_RDD2.reduceByKey((x, y) => {
      (
        x._1,
        x._2,
        x._3,
        x._4,
        x._5
      )
    }).values

    facebook_RDD3.foreach(println)


    val ret = spark.createDataFrame(facebook_RDD3).withColumnRenamed("_1", "id_date").withColumnRenamed("_2", "canal_e")
      .withColumnRenamed("_3", "organico").withColumnRenamed("_4", "pagado").withColumnRenamed("_5", "id_load_date")
      .withColumn("id_load_date", to_utc_timestamp(from_unixtime(unix_timestamp()), "ES"))

    ret.rdd.take(50).foreach(println)

    ret
  }

  def RedesSocialesLoad(conf: Map[String, String], finalDF: Dataset[Row]): String ={
    println("CARGANDO EN TABLA")
    val dbProps = new Properties
    dbProps.setProperty("user", conf("DB_USER"))
    dbProps.setProperty("ssl", "true")
    dbProps.setProperty("sslmode", conf("SSL_MODE"))
    dbProps.setProperty("sslcert", conf("SSLCERT"))
    dbProps.setProperty("sslkey", conf("SSLKEY"))
    dbProps.setProperty("sslrootcert", conf("SSLROOTCERT"))
    dbProps.setProperty("prepareThreshold", "0")
    dbProps.setProperty("driver", "org.postgresql.Driver")
    val dbURL = conf("DB_URL")
    println(finalDF.count()+">>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    finalDF.write.mode(SaveMode.Append).jdbc(dbURL, "cmd.f_alcance_rrss_test", dbProps)

    "EJECUCIÓN CORRECTA"


  }

  def main(args: Array[String]): Unit = {

    val id_load_date = (System.currentTimeMillis / 1000).toString


    val db_URL = "jdbc:postgresql://poolpostgresqlwithsec.marathon.mesos:5432/bi_digital" + "?prepareThreshold=0"
    val db_user = "eos.segm.batch"
    val db_pass = ""




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



    val spark = SparkSession.builder().master("local").appName("MongoTest").getOrCreate()
//    val sslCert = spark.conf.get("spark.ssl.datastore.certPem.path")
//    val sslKey = spark.conf.get("spark.ssl.datastore.keyPKCS8.path")
//    val sslRootCert = spark.conf.get("spark.ssl.datastore.caPem.path")
 //   val sslmode = "verify-full"

    // Mapa de configuraciones

    val conf = Map(

      "MONGO_TRANSFaORMATION" -> mongoUri,
      "FACEBOOKDB" -> database,
      "FACEBOOKCOLLECTION" -> collection,
      "FACEBOOK_START_DATE" -> fmt.print(start_date),
      "ID_LOAD_DATE" -> id_load_date,
      "FACEBOOK_END_DATE" -> fmt.print(end_date),
      "FACEBOOOK_TIMESPAN" -> "RANGEDATE",
      "DB_URL" -> db_URL,
      "DB_USER" -> db_user,
      "DB_PASS" -> db_pass,
      "SSL" -> "true"
  //    "SSLROOTCERT" -> sslRootCert,
  //    "SSL_MODE" -> sslmode,
  //    "SSLCERT" -> sslCert,
  //    "SSLKEY" -> sslKey
    )




    val impressions =  extractFBImpressions(spark, conf);


    val finalDF = transformFBImpressions(impressions, spark, conf)



    RedesSocialesLoad(conf, finalDF)


  }


}
