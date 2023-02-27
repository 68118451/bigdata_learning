package com.flink.example.skyeye.sink

import com.flink.example.skyeye.SkyeyeLogEventToES.{USER, PASSWORD, URL,getAppMap}
import com.flink.example.skyeye.modul.{SkyeyeLogESMapper, SkyeyeLogPushES}
import com.flink.sink.es.ESClientSingleton
import com.flink.utils.DingTalkUtil
import com.google.common.base.Strings
import com.suishen.elasticsearch.core.query.{BulkRequest, UpdateQuery}
import com.suishen.elasticsearch.rest.client.RestHighLevelClient
import com.suishen.elasticsearch.rest.client.config.HttpHostConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.elasticsearch.common.unit.TimeValue
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.Date
import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable

class ElasticSearchLogSink extends RichSinkFunction[SkyeyeLogPushES]{

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ElasticSearchLogSink])
  private val DEFAULT_BULK_SIZE: Int = 3000

  private var timer: ScheduledExecutorService = _
  private val flushIntervalSeconds = 5
  private var esClusterConfigs: util.List[HttpHostConfig] = _
  private var flushBufferSize: Int = 0
  private var bulkSize: Int = DEFAULT_BULK_SIZE
  private var client: RestHighLevelClient = _
  private var mapperBuffer: util.HashSet[SkyeyeLogESMapper] = _
  private var esIndex: String = _
  private var esType: String = _
  private var appMap:mutable.HashMap[String, (String,String,String,String)] = _
  private var failSize: Int = 0
  private val DEFAULT_FAILE_SIZE: Int = 10


  override def invoke(value: SkyeyeLogPushES): Unit = {
    val mapper: SkyeyeLogESMapper = rowToMapper(value)
    mapperBuffer.add(mapper)
    flushBufferSize += 1
    checkFlush()
  }


  override def open(parameters: Configuration): Unit = {
    this.mapperBuffer = new util.HashSet[SkyeyeLogESMapper]
    try {
      //            LOG.error(s"initial ES RestHighLevelClient start! client:[$client],  esClusterConfigs:[$esClusterConfigs]")
      client = ESClientSingleton.getInstance(esClusterConfigs)


      //构建es客户端连接，设置超时间
      //       client = RestClient.builder(new HttpHost("node1.es.all.ops.wtc.hwhostname.com", 9200, "http")).setMaxRetryTimeoutMillis(5 * 60 * 1000).build

      //            LOG.error(s"initial ES RestHighLevelClient success! client:[$client]")

    } catch {
      //抛出throw new RuntimeException(e)异常后，task执行失败重试
      case e: Exception =>
        LOG.error("initial ES RestHighLevelClient error", e)
        throw new RuntimeException(e)
    }


    try {
      //注册定时器，每10秒检测一下是否有数据，有数据则写入es
      timer = new ScheduledThreadPoolExecutor(5)
      timer.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          bulkRequest()
        }
      }, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS)

    } catch {
      case e: Exception =>
        LOG.error("es请求错误批量数据大小:" + flushBufferSize)
        LOG.error("esSinkTimer execute error", e)

        //执行失败重试
        if(failSize <= DEFAULT_FAILE_SIZE){
          bulkRequest()
        }

      //        throw new RuntimeException(e)
    }

  }


  override def close(): Unit = {
    if (flushBufferSize > 0) {
      //            LOG.error("close:ES flush start")
      bulkRequest()
      //            LOG.error("close:ES flush success")
    }
  }

  def this(esClusterConfigs: util.List[HttpHostConfig]) {
    this()
    this.esClusterConfigs = esClusterConfigs
  }


  def withEsClusterConfigs(esClusterConfigs: util.List[HttpHostConfig]): ElasticSearchLogSink = {
    this.esClusterConfigs = esClusterConfigs
    this
  }


  def withBulkSize(bulkSize: Int): ElasticSearchLogSink = {
    this.bulkSize = bulkSize
    this
  }

  def withEsIndex(index: String): ElasticSearchLogSink = {
    this.esIndex = index
    this
  }

  def withEsType(`type`: String): ElasticSearchLogSink = {
    this.esType = `type`
    this
  }

  def withAppMap(appmap: mutable.HashMap[String, (String,String,String,String)] ): ElasticSearchLogSink = {
    this.appMap = appmap
    this
  }

  private def checkFlush(): Unit = {
    try {
      if (flushBufferSize > bulkSize) {
        //                LOG.error("size:ES flush start")
        bulkRequest()
        //                LOG.error("size:ES flush success")
      }
    } catch {
      case e: Exception =>
        LOG.error("es请求错误批量数据大小:" + flushBufferSize)
        LOG.error("elasticsearchFlush execute error", e)
        //执行失败重试
        if(failSize <= DEFAULT_FAILE_SIZE){
          bulkRequest()
        }
      //        throw new RuntimeException(e)
    }
  }

  //调用发送钉钉告警流程
  def sendLogToDing(flushBufferSize:Int,esIndex:String,failSize:Int,flag:Int): Unit ={

    val time: String = new DateTime(new Date().getTime()).toString("yyyy-MM-dd HH:mm")
    val title: StringBuilder = new StringBuilder
    title.append("SkyeyeLogEventToES触发告警:")

    val text: StringBuilder = new StringBuilder
    if(flag == 0){
      text.append("报错索引:" + esIndex + "\n")
      text.append("es请求错误批量数据大小:" + flushBufferSize + "\n")
      text.append("失败重试次数:" + failSize + "\n")
    }else{
      text.append("es请求错误批量数据大小:" + flushBufferSize + "\n")
      text.append("失败重试次数:" + failSize + "\n")
      text.append("失败时间:" + time + "\n")
      text.append("esSink连续无法执行" + failSize + "次@徐伟奇"  + "\n")
    }


    DingTalkUtil.sendAlarm(title.toString, text.toString, "https://oapi.dingtalk.com/robot/send?access_token=79b8180144e91a00724aff5110a385a7df756ef4390dffb11cc11ec0a67d41c4")
  }


  private def rowToMapper(row: SkyeyeLogPushES): SkyeyeLogESMapper = {
    val mapper = new SkyeyeLogESMapper
    mapper.setEventId(Strings.nullToEmpty(row.event_id))
    mapper.setAppKey(Strings.nullToEmpty(row.app_key))
    mapper.setApplication(Strings.nullToEmpty(row.application))
    mapper.setAppName(Strings.nullToEmpty(row.app_name))
    mapper.setModule(Strings.nullToEmpty(row.module))
    mapper.setlevel(Strings.nullToEmpty(row.level))
    mapper.setTitle(Strings.nullToEmpty(row.title))
    mapper.setContent(Strings.nullToEmpty(row.content))
    mapper.setDeviceId(Strings.nullToEmpty(row.device_id))
    mapper.setModel(Strings.nullToEmpty(row.model))
    mapper.setAppVersion(Strings.nullToEmpty(row.app_version))
    mapper.setAppVersionCode(Strings.nullToEmpty(row.app_ver_code))
    mapper.setSdkVersion(Strings.nullToEmpty(row.sdk_version))
    mapper.setOs(Strings.nullToEmpty(row.os))
    mapper.setOsVersion(Strings.nullToEmpty(row.os_version))
    mapper.setR(Strings.nullToEmpty(row.r))
    mapper.setNetwork(Strings.nullToEmpty(row.network))
    mapper.setSp(Strings.nullToEmpty(row.sp))
    mapper.setCity(Strings.nullToEmpty(row.city))
    mapper.setChannel(Strings.nullToEmpty(row.channel))
    mapper.setImei(Strings.nullToEmpty(row.imei))
    mapper.setOaid(Strings.nullToEmpty(row.oaid))
    mapper.setDfid(Strings.nullToEmpty(row.dfid))
    mapper.setIdfa(Strings.nullToEmpty(row.idfa))
    mapper.setUid(Strings.nullToEmpty(row.uid))
    mapper.setEventTime(row.event_time)
    mapper.setPkg(Strings.nullToEmpty(row.pkg))
    mapper.setPlatform(Strings.nullToEmpty(row.platform))
    mapper
  }

  protected def bulkRequest(): Unit = {
    if (mapperBuffer.isEmpty) return

    //        val startTime: Long = System.currentTimeMillis
    val bulkRequest: BulkRequest = new BulkRequest
    var updateQuery: UpdateQuery = null

    import scala.collection.JavaConversions._
    for (mapper <- mapperBuffer) { //; ;mapper != null
      //当传入的appMap不为空时获取每条数据的app_key,以mysql查出来的数据对应application来拼接索引
      if(appMap != null){
        val app_key: String = mapper.getAppKey
        var application = ""
        if (appMap.contains(app_key)) {
          application = appMap.get(app_key).getOrElse(("other","","",""))._1
        }else{
          //如果app_key获取不到对应的数据则更新appMap,再获取不到数据则写入skyeye-other-log索引中,防止出错任务停止
          LOG.info("数据库更新！")
          appMap = getAppMap(URL, USER, PASSWORD)
          application = appMap.get(app_key).getOrElse(("other","","",""))._1
        }
        esIndex = "skyeye-"+application.toLowerCase+"-log"

        if ("skyeye-zhwnl-log".equals(esIndex)){
          esIndex = "skyeye-zhwnl-log2"
        }

        //        println(esIndex)
        esType = "skyeye-event-log"
      }
      if (null != mapper) {
        updateQuery = UpdateQuery
          .newBuilder
          .setId(mapper.getEventId)
          .setObject(mapper)
          .setIndexName(this.esIndex)
          .setType(this.esType)
          .setIsUpsert(true)
          .build
        bulkRequest.add(updateQuery)
      }
    }
    if (!bulkRequest.isEmpty) {
      LOG.info("elasticsearch bulk request start")

      val success: Boolean = client.bulk(bulkRequest, new TimeValue(180, TimeUnit.SECONDS))
      if (!success) {
        LOG.error("elasticsearch bulk request fail")
        LOG.error("报错索引：" + esIndex)
        LOG.error("报错时数据库数据：" + appMap)
        LOG.error("报错时写入数据：" + mapperBuffer.toString)
        failSize += 1
        LOG.error("失败重试次数：" + failSize)
        //写入es失败时发送钉钉告警
        //        sendLogToDing(flushBufferSize,esIndex,failSize,0)

        //当连续失败5次发送钉钉告警,清空当前缓存数据,重置失败次数
        if(failSize > DEFAULT_FAILE_SIZE){
          sendLogToDing(flushBufferSize,esIndex,failSize,1)
          mapperBuffer.clear()
          flushBufferSize = 0
          failSize = 0
        }

        //        throw new RuntimeException("elasticsearch bulk request fail")
      }else{
        LOG.info("elasticsearch bulk request success")
        LOG.info("成功写入es索引:" + esIndex + " 写入数据大小:" + flushBufferSize)
        //        LOG.info("成功写入es数据大小:" + flushBufferSize)
        mapperBuffer.clear()
        flushBufferSize = 0
        failSize = 0

      }

    }
  }

}
