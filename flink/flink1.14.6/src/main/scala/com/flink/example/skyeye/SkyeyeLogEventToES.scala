package com.flink.example.skyeye

import com.flink.`type`.SourceAndSinkType
import com.flink.conf.AppConf
import com.flink.example.skyeye.function.{SkyeyeLogEventFilterApp, SkyeyeLogEventFilterGame, SkyeyeLogEventMap}
import com.flink.example.skyeye.modul.{SkyeyeLog, SkyeyeLogPushES}
import com.flink.example.skyeye.serialize.SkyeyeSerializeSchema
import com.flink.example.skyeye.sink.ElasticSearchLogSink
import com.flink.example.skyeye.utils.MysqlUtils
import com.flink.serialize.ProtobufDeserialize
import com.suishen.elasticsearch.rest.client.config.HttpHostConfig
import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.sql.ResultSet
import java.util.Properties
import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable

object SkyeyeLogEventToES {

  private val appConf = AppConf()
  private var timer: ScheduledExecutorService = _
  private val flushIntervalMinutes = 1440
  private val KAFKA_CONSUMER_GROUP_ID = "skyeye-log-event-to-es-test"
  private val KAFKA_CONSUMER_TOPIC = appConf.getSkyeyeKafkaConsumerTopic
  private val KAFKA_PRODUCER_TOPIC = appConf.getSkyeyeKafkaProducerTopic
  private val KAFKA_BOOTSTRAP_SERVERS = appConf.getSkyeyeKafkaBootstrapServers
  private val ES_HOST = appConf.getSkyeyeESHost
  private val RETRIES_CONFIG = "4"
  val URL: String = appConf.getSkyeyeMysqlJdbcUrl
  val USER: String = appConf.getSkyeyeMysqlUser
  val PASSWORD: String = appConf.getSkyeyeMysqlPassWord
  val bulkFlushSize = 7000
  var appMap: mutable.HashMap[String, (String, String, String, String)] = _


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.registerTypeWithKryoSerializer(classOf[SkyeyeLog.Entity], classOf[ProtobufSerializer])
    env.setParallelism(15)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000))

    //    env.enableCheckpointing(TimeUnit.MINUTES.toMillis(20))
    //    val checkpointConf = env.getCheckpointConfig
    //    checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    checkpointConf.setMinPauseBetweenCheckpoints(50000L)
    //    checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(20))
    //    checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val properties = new Properties
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP_ID)
    properties.setProperty("flink.partition-discovery.interval-millis", TimeUnit.MINUTES.toMillis(1).toString)

    val producerProps = new Properties
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS)
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG)
    producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "1")

    appMap = getAppMap(URL, USER, PASSWORD)

    //注册定时器，定时更新mysql数据库数据(每天更新一次)
    timer = new ScheduledThreadPoolExecutor(1)
    timer.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        appMap = getAppMap(URL, USER, PASSWORD)
      }
    }, flushIntervalMinutes, flushIntervalMinutes, TimeUnit.MINUTES)


    val logKafkaConsumer: FlinkKafkaConsumerBase[SkyeyeLog.Entity] =
      new FlinkKafkaConsumer[SkyeyeLog.Entity](KAFKA_CONSUMER_TOPIC, new ProtobufDeserialize(classOf[SkyeyeLog.Entity]), properties)
        .setStartFromGroupOffsets()

    val logKafkaProducer: FlinkKafkaProducer[SkyeyeLogPushES] =
      new FlinkKafkaProducer[SkyeyeLogPushES](KAFKA_PRODUCER_TOPIC, new SkyeyeSerializeSchema(KAFKA_PRODUCER_TOPIC), producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

    //将Kafka数据转换成SkyeyeLogPushES类型
    val logEventDS: DataStream[SkyeyeLogPushES] = env
      .addSource(logKafkaConsumer)
      .name(SourceAndSinkType.KAFKA_TOPIC + KAFKA_CONSUMER_TOPIC)
      .filter(_ != null)
      .name("filter-null-log")
      .filter(_.getAppKey != null)
      .name("filter-null-appkey")
      .map(new SkyeyeLogEventMap(appMap))
      .name("map-log-event")


    //将数据分流转换成json格式写入另一个topic供后端使用
    logEventDS
      .addSink(logKafkaProducer)
      .name(SourceAndSinkType.KAFKA_TOPIC + KAFKA_PRODUCER_TOPIC)

    //过滤出游戏,写入skyeye-game-log索引中
    logEventDS
      .filter(new SkyeyeLogEventFilterGame(appMap))
      .name("filter-game-log")
      .addSink(makeEsIndexLogSink("skyeye-game-log", "skyeye-event-log"))
      .name(SourceAndSinkType.ELASTICSEARCH + "index:skyeye-game-log,type:skyeye-event-log")

    //过滤出app,写入skyeye-{application}-log索引中(根据application自动拼接索引)
    logEventDS
      .filter(new SkyeyeLogEventFilterApp(appMap))
      .name("filter-app-log")
      .addSink(makeEsIndexLogSink(appMap))
      .name(SourceAndSinkType.ELASTICSEARCH + "index:skyeye-{application}-log,type:skyeye-event-log")


    env.execute("skyeye-log-event-to-es")

  }

  def getAppMap(url: String, user: String, password: String): mutable.HashMap[String, (String, String, String, String)] = {
    val appMap: mutable.HashMap[String, (String, String, String, String)] = new mutable.HashMap[String, (String, String, String, String)]
    Class.forName("com.mysql.jdbc.Driver")
    //1、建立mysql连接
    val ssy_app: MysqlUtils = new MysqlUtils(url, user, password)
    //2、获取ssy_app表中app_key与application,platform,`type`的映射关系
    val resultSet: ResultSet = ssy_app.executeQuery("select app_key,application,name,platform,`type` from ssy_app")
    appMap.clear()

    //3、将数据存入appMap中
    while (resultSet.next()) {

      val app_key: String = resultSet.getString(1)
      val application: String = resultSet.getString(2)
      val app_name: String = resultSet.getString(3)
      val platform: String = resultSet.getString(4)
      val `type`: String = resultSet.getString(5)
      appMap.put(app_key, (application, app_name, platform, `type`))
    }

    resultSet.close()
    ssy_app.close()
    appMap

  }

  protected def makeEsIndexLogSink(index: String, `type`: String): ElasticSearchLogSink = {
    val esClusterConfigs: java.util.List[HttpHostConfig] = new java.util.ArrayList[HttpHostConfig]
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(0), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(1), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(2), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(3), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(4), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(5), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(6), 9200, "http"))
    new ElasticSearchLogSink(esClusterConfigs)
      .withBulkSize(bulkFlushSize)
      .withEsIndex(index)
      .withEsType(`type`)
  }

  protected def makeEsIndexLogSink(appmap: mutable.HashMap[String, (String, String, String, String)]): ElasticSearchLogSink = {
    val esClusterConfigs: java.util.List[HttpHostConfig] = new java.util.ArrayList[HttpHostConfig]
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(0), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(1), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(2), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(3), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(4), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(5), 9200, "http"))
    esClusterConfigs.add(new HttpHostConfig(ES_HOST.split(",")(6), 9200, "http"))
    new ElasticSearchLogSink(esClusterConfigs)
      .withBulkSize(bulkFlushSize)
      .withAppMap(appmap)

  }


}

