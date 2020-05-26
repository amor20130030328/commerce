import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {

  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {

    val key2NumDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timestamp = logSplit(0).toLong
        val dateKey = DateUtils.formatDateKey(new Date(timestamp))
        val userId = logSplit(3).toLong
        val adId = logSplit(4).toLong
        val key = dateKey + "_" + userId + "_" + adId
        (key, 1L)
    }

    val key2CountDStream = key2NumDStream.reduceByKey(_+_)
    //根据每一个RDD里面的数据，更新用户点击次数表
    key2CountDStream.foreachRDD(
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()
          for((key,count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adId = keySplit(2).toLong
            clickCountArray += AdUserClickCount(date , userId, adId, count)
          }
          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    )

    val key2BlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adId = keySplit(2).toLong
        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adId)
        if (clickCount > 100) {
          true
        } else {
          false
        }
    }
    val userIdDStream = key2BlackListDStream.map {
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()
          for(userId <- items){
            userIdArray += AdBlacklist(userId)
          }

          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }


    }




  }

  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {

    val key2ProvinceCityDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timestamp = logSplit(0).toLong
        //dateKey : yyyy-MM-dd
        val dateKey = DateUtils.formatDateKey(new Date(timestamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adId = logSplit(4)
        val key = dateKey + "_" + province + "_" + city + "_" + adId
        (key, 1L)
    }

    //某一天一个省的一个城市中某一个广告的点击次数（累积）
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get
        for (value <- values) {
          newValue += value
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray = new ArrayBuffer[AdStat]()
          // key : date province city adId
          for((key,count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adId = keySplit(3).toLong
            adStatArray += AdStat(date,province,city,adId,count)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }
    key2StateDStream
  }

  def provinceTop3Adver(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {

    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adId = keySplit(3)
        val newKey = date + "_" + province + "_" + adId
        (newKey, count)
    }
    val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_+_)
    val top3DStream = key2ProvinceAggrCountDStream.transform {
      rdd =>
        val basicDataRDD = rdd.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adId = keySplit(2).toLong
            (date, province, adId, count)
        }

        import sparkSession.implicits._
        basicDataRDD.toDF("date", "province", "adId", "count").createOrReplaceTempView("tmp_basic_info")
        val sql = "select date , province , adId , count from (" +
          " select date , province ,adId ,count, row_number() over(partition by date , province order by count desc ) rank from tmp_basic_info ) t " +
          " where rank <= 3 "
        sparkSession.sql(sql).rdd
    }
    top3DStream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
         items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for(item <- items){
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adId = item.getAs[Long]("adId")
              val count = item.getAs[Long]("count")
              top3Array += AdProvinceTop3(date,province,adId,count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }


  }

  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {

    val key2TimeMinuteDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timestamp = logSplit(0).toLong
        val timeMinute = DateUtils.formatTimeMinute(new Date(timestamp))
        val adId = logSplit(4).toLong
        val key = timeMinute + "_" + adId
        (key, 1L)
    }
    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a:Long , b :Long) => (a+b) , Minutes(60), Minutes(1))

    key2WindowDStream.foreachRDD{
      rdd => rdd.foreachPartition{
          items =>
          val trendArray = new ArrayBuffer[AdClickTrend]()
          for((key,count) <- items){
            val keySplit = key.split("_")
            val timeMinute = keySplit(0)
            val date = timeMinute.substring(0,8)
            val hour = timeMinute.substring(8,10)
            val minute = timeMinute.substring(10)
            val adId = keySplit(1).toLong
            trendArray += AdClickTrend(date,hour,minute,adId,count)
          }
          AdClickTrendDAO.updateBatch(trendArray.toArray)
      }
    }



  }

  def main(args: Array[String]): Unit = {


      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("adver")
      val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


      // val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir,func)
      val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))
      val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
      val kafka_brokers = "cm102:9092,cm103:9092,cm104:9092" /*ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)*/

      val kafkaParam = Map(
        "bootstrap.servers" -> kafka_brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "group1",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false : java.lang.Boolean)

        /**
          *   auto.offset.reset
          *   latest : 先去Zookeeper获取offset ,如果有，直接使用，如果没有，从最新数据开始消费
          *   earlist :先去Zookeeper获取 offset, 如果有，直接使用，如果没有，从最开始的数据开始消费
          *   none : 先去Zookeeper 获取offset ,如果有，直接使用，如果没有，直接报错
          */
      )

    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    val adRealTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream = adRealTimeValueDStream.transform {
      logRDD =>
        val blacklistArray = AdBlacklistDAO.findAll()
        val userIdArray = blacklistArray.map(item => item.userid)
        logRDD.filter {
          case log => //log : timestamp province city userid adid
            val userId = log.split(" ")(3).toLong
            !userIdArray.contains(userId)
        }
    }

    streamingContext.checkpoint("./provinceCityadstat")
    adRealTimeFilterDStream.checkpoint(Duration(10000))

    //需求一：实时维护黑名单
    generateBlackList(adRealTimeFilterDStream)

    //需求二 : 各省市一天中的广告点击量（累积统计）
    val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

    //需求三:统计各省Top3热门广告
    provinceTop3Adver(sparkSession , key2ProvinceCityCountDStream)

    //需求四:最近一小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)


    streamingContext.start()
    streamingContext.awaitTermination()


  }
}
