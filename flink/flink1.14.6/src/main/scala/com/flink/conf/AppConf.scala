package com.flink.conf


class AppConf extends CommonConf  {

  load()

  val ACTIVE_USER_TOPIC_KEY = "active.user.topic"

  val NEW_USER_TOPIC_KEY = "new.user.topic"

  val WOLVES_EVENT = "wolves.event"

  val WOLVES_NEW_ANDROIDID = "wolves.new.androidid"

  val WOLVES_RETAIN_TOPIC_KEY = "wolves.retain.topic"

  val WOLVES_ANDROID_ID_RETAIN_TOPIC_KEY = "wolves.retain.androidid"

  val WOLVES_UID_USER_TOPIC_KEY = "wolves.uid.user"

  val WOLVES_UID_USER_ANDROID_ID_TOPIC_KEY = "wolves.uid.androidid.user"

  val WOLVES_KEY_BEHAVIOR_USER = "wolves.key.behavior.user"

  val WOLVES_CONVERSION_EVENT = "wolves.conversion.event"

  val WOLVES_CONVERSION_REGISTER_EVENT = "wolves.conversion.register.event"

  val WOLVES_CONVERSION_RETAIN_EVENT = "wolves.conversion.retain.event"

  val HIVE_LINEAGE_TOPIC_KEY = "hive.lineage"

  val DSP_V3_REPORT_LOG_BIN = "dsp.v3.report.log.bin"

  val ROOSTER_ES_HOST = "elasticsearch.host"

  val ROOSTER_DMP_M1 = "rooster.dmp.m1"

  val ROOSTER_DMP_S1 = "rooster.dmp.s1"

  val REDIS_MASTER_URL = "m1.redis.rose.rosehosts.com"

  val REDIS_SLAVE_URL = "s1.redis.rose.rosehosts.com"

  val WOLVES_REDIS_MASTER_URL = "m1.redis.wolves.dmp.com"

  val WOLVES_REDIS_SLAVE_URL = "s1.redis.wolves.dmp.com"

  val CYBERCAT_REDIS_MASTER_URL = "m1.redis.cybercat.cybercathosts.com"

  val CYBERCAT_REDIS_SLAVE_URL = "s1.redis.cybercat.cybercathosts.com"

  val SQUIRREL_REDIS_MASTER_URL = "m1.redis.squirrel.squirrelhosts.com"

  val SQUIRREL_REDIS_SLAVE_URL = "s1.redis.squirrel.squirrelhosts.com"

  val FILTER1_REDIS_MASTER_URL = "m1.redis.filter.bigdata.wtc.hwhosts.com"

  val FILTER1_REDIS_SLAVE_URL = "s1.redis.filter.bigdata.wtc.hwhosts.com"

  val FILTER2_REDIS_MASTER_URL = "m2.redis.filter.bigdata.wtc.hwhosts.com"

  val FILTER2_REDIS_SLAVE_URL = "s2.redis.filter.bigdata.wtc.hwhosts.com"

  val FILTER3_REDIS_MASTER_URL = "m3.redis.filter.bigdata.wtc.hwhosts.com"

  val FILTER3_REDIS_SLAVE_URL = "s3.redis.filter.bigdata.wtc.hwhosts.com"

  val WOLVES_MYSQL_JDBC_URL = "wolves.mysql.jdbc.url"

  val WOLVES_MYSQL_USER = "wolves.mysql.user"

  val WOLVES_MYSQL_PASSWORD = "wolves.mysql.password"

  /**
   * Skyeye 业务配置
   */
  val SKYEYE_KAFKA_BOOTSTRAP_SERVERS = "skyeye.kafka.bootstrap.servers"
  val SKYEYE_ES_HOST = "skyeye.es.host"
  val SKYEYE_MYSQL_JDBC_URL = "skyeye.mysql.jdbc.url"
  val SKYEYE_MYSQL_USER = "skyeye.mysql.user"
  val SKYEYE_MYSQL_PASSWARD = "skyeye.mysql.password"
  val SKYEYE_KAFKA_CONSUMER_TOPIC = "skyeye.kafka.consumer.topic"
  val SKYEYE_KAFKA_PRODUCER_TOPIC = "skyeye.kafka.producer.topic"

  /**
   * neo4j conf key
   */
  val NEO4J_URI = "neo4j.url"

  val NEO4J_USERNAME = "neo4j.username"

  val NEO4J_PASSWORD = "neo4j.password"

  /**
   * canal业务配置
   */
  val CANAL_ADMIN_URI = "canalAdmin.url"

  val CANAL_ADMIN_USERNAME = "canalAdmin.username"

  val CANAL_ADMIN_PASSWORD = "canalAdmin.password"

  def getActiveUserTopic: String = param.get(ACTIVE_USER_TOPIC_KEY)

  def getNewUserTopic: String = param.get(NEW_USER_TOPIC_KEY)

  def getWolvesEventTopic: String = param.get(WOLVES_EVENT)

  def getWolvesNewAndroidIdTopic: String = param.get(WOLVES_NEW_ANDROIDID)

  def getWolvesRetainTopic: String = param.get(WOLVES_RETAIN_TOPIC_KEY)

  def getWolvesAndroidIdRetainTopic: String = param.get(WOLVES_ANDROID_ID_RETAIN_TOPIC_KEY)

  def getWolvesUidUserTopic: String = param.get(WOLVES_UID_USER_TOPIC_KEY)

  def getWolvesUidAndroidIdUserTopic: String = param.get(WOLVES_UID_USER_ANDROID_ID_TOPIC_KEY)

  def getWolvesKeyBehaviorUserTopic: String = param.get(WOLVES_KEY_BEHAVIOR_USER)

  def getWolvesConversionEventTopic: String = param.get(WOLVES_CONVERSION_EVENT)

  def getWolvesConversionRegisterEventTopic: String = param.get(WOLVES_CONVERSION_REGISTER_EVENT)

  def getWolvesConversionRetainEventTopic: String = param.get(WOLVES_CONVERSION_RETAIN_EVENT)

  def getHiveLineageInfoTopic: String = param.get(HIVE_LINEAGE_TOPIC_KEY)

  def getElasticsearchHost: String = param.get(ROOSTER_ES_HOST)

  def getRoosterDmpM1: String = param.get(ROOSTER_DMP_M1)

  def getRoosterDmpS1: String = param.get(ROOSTER_DMP_S1)

  def getRedisMasterUrl: String = param.get(REDIS_MASTER_URL)

  def getRedisSlaveUrl: String = param.get(REDIS_SLAVE_URL)

  def getWolvesRedisMasterUrl: String = param.get(WOLVES_REDIS_MASTER_URL)

  def getWolvesRedisSlaveUrl: String = param.get(WOLVES_REDIS_SLAVE_URL)

  def getCybercatRedisMasterUrl: String = param.get(CYBERCAT_REDIS_MASTER_URL)

  def getCybercatRedisSlaveUrl: String = param.get(CYBERCAT_REDIS_SLAVE_URL)

  def getSquirrelRedisMasterUrl: String = param.get(SQUIRREL_REDIS_MASTER_URL)

  def getSquirrelRedisSlaveUrl: String = param.get(SQUIRREL_REDIS_SLAVE_URL)

  def getFilter1RedisMasterUrl: String = param.get(FILTER1_REDIS_MASTER_URL)

  def getFilter1RedisSlaveUrl: String = param.get(FILTER1_REDIS_SLAVE_URL)

  def getFilter2RedisMasterUrl: String = param.get(FILTER2_REDIS_MASTER_URL)

  def getFilter2RedisSlaveUrl: String = param.get(FILTER2_REDIS_SLAVE_URL)

  def getFilter3RedisMasterUrl: String = param.get(FILTER3_REDIS_MASTER_URL)

  def getFilter3RedisSlaveUrl: String = param.get(FILTER3_REDIS_SLAVE_URL)

  def getNeo4jUri: String = param.get(NEO4J_URI)

  def getNeo4jUsername: String = param.get(NEO4J_USERNAME)

  def getNeo4jPassword: String = param.get(NEO4J_PASSWORD)

  def getCanalAdminUri: String = param.get(CANAL_ADMIN_URI)

  def getCanalAdminUsername: String = param.get(CANAL_ADMIN_USERNAME)

  def getCanalAdminPassword: String = param.get(CANAL_ADMIN_PASSWORD)

  def getDspV3ReportLogBin: String = param.get(DSP_V3_REPORT_LOG_BIN)

  def getWolvesMysqlJdbcUrl: String = param.get(WOLVES_MYSQL_JDBC_URL)

  def getWolvesMysqlUser: String = param.get(WOLVES_MYSQL_USER)

  def getWolvesMysqlPassWord: String = param.get(WOLVES_MYSQL_PASSWORD)

  def getSkyeyeKafkaBootstrapServers :String = param.get(SKYEYE_KAFKA_BOOTSTRAP_SERVERS)

  def getSkyeyeESHost :String = param.get(SKYEYE_ES_HOST)

  def getSkyeyeMysqlJdbcUrl :String = param.get(SKYEYE_MYSQL_JDBC_URL)

  def getSkyeyeMysqlUser :String = param.get(SKYEYE_MYSQL_USER)

  def getSkyeyeMysqlPassWord :String = param.get(SKYEYE_MYSQL_PASSWARD)

  def getSkyeyeKafkaConsumerTopic :String = param.get(SKYEYE_KAFKA_CONSUMER_TOPIC)

  def getSkyeyeKafkaProducerTopic :String = param.get(SKYEYE_KAFKA_PRODUCER_TOPIC)

}


object AppConf extends Serializable {

  def apply(): AppConf = {
    new AppConf()
  }

}