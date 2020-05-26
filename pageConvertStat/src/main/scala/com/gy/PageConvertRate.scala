package com.gy

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, NumberUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * 页面单跳转化率模块spark作业
  * 页面转化率的求解思路通过UserAction 表获取一个session的所有UserAction,根据时间顺序排序后获取全部的pageId
  * 然后将PageId成PageFlow，即1,2,3,4,5的形式（按照时间顺序排列），之后，组合为1_3,2_3,3_4，... 的形式然后
  * 筛选出出现在targetFlow 的所有A_B
  * 对每个A_B进行数量统计，然后统计startPage的PV，之后根据targetFlow 的A_B顺序，计算每一层的转化率
  *
  */
object PageConvertRate {
  def getActionRDDByDateRange(sparkSession: SparkSession, taskParam: JSONObject)= {

    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    import  sparkSession.implicits._
    sparkSession.sql("use commerce")
    val sql = "select * from user_visit_action where date >='" + startDate +"' and date <='" + endDate +"'"
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }


  def generateAndMatchPageSplit(sc: SparkContext, sessionId2ActionsRDD: RDD[(String, Iterable[UserVisitAction])], taskParam: JSONObject) = {

    //对目标pageFlow进行解析
    val targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    //将字符串转换成为了List[String]
    val targetPages = targetPageFlow.split(",").toList

    val targetPagePairs = targetPages.slice(0, targetPages.length - 1).zip(targetPages.tail).map(item => item._1+"_" + item._2)

    val targetPageFlowBroadcast = sc.broadcast(targetPagePairs)
    /*************对所有PageFlow进行解析******************/
    /**************对所有PageFlow进行解析****************/

    sessionId2ActionsRDD.flatMap {
      case (sessionId, userVisitActions) =>
        //获取使用者指定的页面流
        //使用者指定的页面流，1,2,3,4,5,6,7
        //1->2的转化率是多少，? 2->3的转化率是多少
        //这里，我们拿到的session的访问行为，默认情况下是乱序的
        //比如说，正常情况下，我们希望拿到的数据，是按照时间乱序排序的
        //但是问题是，默认是不排序的
        //所以，我们第一件事情，对session的访问行为数据是按照时间进行排序
        //举例，反例
        //比如,3-5->4->10->7
        //3->4->5->7->10
        val sortedUVAs = userVisitActions.toList.sortWith((uva1, uva2) => DateUtils.parseTime(uva1.action_time).getTime < DateUtils.parseTime(uva2.action_time).getTime)
        //提取所有UserAction 中的PageId信息
        val sortedPages = sortedUVAs.map(item => if (item.page_id != null) item.page_id)
        //[注意]页面的PageFlow是将session的所有UserAction按照时间顺序排序后提取pageId,再将PageId进行连接得到
        //的安装已经排好的顺序对PageId信息进行整合，生成所有页面切片:(1_2,2_3,3_4,4_5,5_6,6_7)
        val sessionPagePairs = sortedPages.slice(0, sortedPages.length - 1).zip(sortedPages.tail).map(item => item._1 + "_" + item._2)

        /* 由此，得到了当前session的PageFlow */
        //只要是当前session的 PageFlow有一个切片与targetPageFlow中任一切片重合，那么就保留下来
        sessionPagePairs.filter(targetPageFlowBroadcast.value.contains(_)).map((_, 1))
    }
  }

  def getStartPage(taskParam: JSONObject, sessionId2ActionsRDD: RDD[(String, Iterable[UserVisitAction])])={

    //获取配置文件中的targetPageFlow
    val targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    //获取起始页面ID
    val startPageId = targetPageFlow.split(",")(0).toLong
    // sessionId2actionsRDD 聚合后的用户行为数据
    val startPageRDD = sessionId2ActionsRDD.flatMap {
      case (sessionId, userVisitActions) =>
        //过滤出所有pageId为startPageId的用户行为数据
        userVisitActions.filter(_.page_id == startPageId).map(_.page_id)
    }
    //对PageId等于startPageId 的用户行为数据进行技术
    startPageRDD.count()
  }

  def computePageSplitConvertRate(taskParam: JSONObject, pageSplitPvMap: collection.Map[String, Long], startPagePv: Long)= {


    val convertRateMap = new mutable.HashMap[String,Double]()

    //1,2,3,4,5,6,7,
    val targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)

    val targetPagtes = targetPageFlow.split(",").toList

    val targetPagePairs = targetPagtes.slice(0,targetPagtes.length-1).zip(targetPagtes.tail).map(item => item._1 +"_" +item._2)
    //lastPageSplitPv : 存储最新一次的页面PV数量
    var lastPageSplitPv = startPagePv.toDouble
    for(targetPage <- targetPagePairs){
      //先获取pageSplitPvMap 中记录的当前targetPage的数量
      val targetPageSplitPv = pageSplitPvMap.get(targetPage).get.toDouble
     
      //用当前targetPage的数量除以上一次lastPageSplit的数量，得到转化率
      
      val convertRate = NumberUtils.formatDouble(targetPageSplitPv / lastPageSplitPv , 2)
      //对targetPage和转化率进行存储
      convertRateMap.put(targetPage , convertRate)
      lastPageSplitPv = targetPageSplitPv
    }
    convertRateMap
  }

  def persistConverRate(sparkSession: SparkSession, taskUUID: String, convertRateMap: mutable.HashMap[String, Double])= {

      val convertRate = convertRateMap.map(item => item._1 +"=" + item._2).mkString("|")

      val pageSplitConvertRateRDD = sparkSession.sparkContext.makeRDD(Array(PageSplitConvertRate(taskUUID,convertRate)))
    import sparkSession.implicits._
    pageSplitConvertRateRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","page_split_convert_rate")
      .mode(SaveMode.Append)
      .save()


  }

  def main(args: Array[String]): Unit = {

    //获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    //任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    //构建spark上下文
    val sparkConf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")
    //创建Spark
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext

    //查询指定日期范围内的用户访问行为数据
    val actionRDD: RDD[UserVisitAction] = this.getActionRDDByDateRange(sparkSession,taskParam)

    //将用户性为信息转换为K-V结构
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id,item))

    //将数据进行内存缓存
    sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY)

    //对<sessionId ,访问行为> RDD，做一次groupByKey操作，生成页面切片
    val sessionId2ActionsRDD = sessionId2ActionRDD.groupByKey()
    //最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
    val pageSplitRDD = generateAndMatchPageSplit(sc,sessionId2ActionsRDD,taskParam)
    //返回: (1_2,1),(3_4,1),...(100_101,1)
    //统计每个跳转切片的总个数
    val pageSplitPvMap = pageSplitRDD.countByKey()
    //使用者指定的页面流是 3,2,5,8,6

    //首选计算首页pv的数量
    val startPagePv = getStartPage(taskParam,sessionId2ActionsRDD)

    //计算目标页面流的各个页面切片的转化率
    val convertRateMap = computePageSplitConvertRate(taskParam,pageSplitPvMap , startPagePv)

    //持久化页面切片转化率
    persistConverRate(sparkSession,taskUUID,convertRateMap)

  }
}
