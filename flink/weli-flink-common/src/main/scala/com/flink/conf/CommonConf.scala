package com.flink.conf

import org.apache.flink.api.java.utils.ParameterTool

class CommonConf extends Serializable{

    /**
      * common conf
      */
    var DEFAULT_CONF_FILENAME = "config.properties"

    var PROD_CONF_FILENAME = "application.properties"

    var TEST_CONF_FILENAME = "test/application.properties"

    var LOCAL_CONF_FILENAME = "dev/application.properties"

    val CLUSTER_MODE = "cluster.mode"

    val CLUSTER_GLOBAL_PARALLELISM = "cluster.global.parallelism"

    val CHECKPOINT_HDFS_DIR = "checkpoint.hdfs.dir"

    /**
      * kafka conf key
      */
    val KAFKA_BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers"

    /**
      * topic key
      */
    val PV_EVENT_TOPIC_KEY = "pv.event.topic"

    val CUSTOM_EVENT_TOPIC_KEY = "custom.event.topic"

    val WEB_EVENT_TOPIC_KEY = "web.event.topic"

    val EVENT_V3_TOPIC_KEY = "event.v3.topic"

    /**
      * hbase kudu aerospike conf key
      */
    val HBASE_QUORUM_ZOOKEEPER_URL_KEY = "hbase.zookeeper.quorum.url"

    val KUDU_MASTER_KEY = "kudu.master"

    val AEROSPIKE_SERVER = "aerospike.server"

    val HIVE_JDBC = "hive.jdbc"


    protected var param: ParameterTool = _

    protected var clusterModeParam = ParameterTool.fromPropertiesFile(this.getClass.getClassLoader.getResourceAsStream(DEFAULT_CONF_FILENAME))

    def getKafkaBootstrapServers: String = param.get(KAFKA_BOOTSTRAP_SERVERS_KEY)

    def getPvEventTopic: String = param.get(PV_EVENT_TOPIC_KEY)

    def getCustomEventTopic: String = param.get(CUSTOM_EVENT_TOPIC_KEY)

    def getWebEventTopic: String = param.get(WEB_EVENT_TOPIC_KEY)

    def getEventV3Topic: String = param.get(EVENT_V3_TOPIC_KEY)

    def getHBaseQuorumZookeeperUrl: String = param.get(HBASE_QUORUM_ZOOKEEPER_URL_KEY)

    def getKuduMaster: String = param.get(KUDU_MASTER_KEY)

    def getAerospikeServer: String = param.get(AEROSPIKE_SERVER)

    def getClusterMode: String = clusterModeParam.get(CLUSTER_MODE)

    def getCheckpointHdfsDir: String = param.get(CHECKPOINT_HDFS_DIR)

    def getHiveJdbc: String = param.get(HIVE_JDBC)

    def getClusterGlobalParallelism(p: Int): Int = {

        val globalParallelism = param.get(CLUSTER_GLOBAL_PARALLELISM)

        if (ClusterMode.prod.name().equals(globalParallelism)
                || ClusterMode.local.name().equals(globalParallelism)) {
            p
        } else {
            globalParallelism.toInt
        }
    }

    def getDynamicKuduZkUrl(): String = {
        val clusterMode = getClusterMode

        if("${cluster.mode}".equals(clusterMode)){
            return "node101.bigdata.dmp.local.com:2181,node102.bigdata.dmp.local.com:2181,node103.bigdata.dmp.local.com:2181"
        }

        ClusterMode.valueOf(clusterMode) match {
            case ClusterMode.test => "test-node1.zk.bigdata.dmp.com:2181,test-node2.zk.bigdata.dmp.com:2181,test-node3.zk.bigdata.dmp.com:2181"
            case ClusterMode.local => "node101.bigdata.dmp.local.com:2181,node102.bigdata.dmp.local.com:2181,node103.bigdata.dmp.local.com:2181"
            case ClusterMode.prod => "10.112.19.227:2181,10.112.25.69:2181,10.112.27.197:2181"
            //case ClusterMode.prod => "node6.zk.bigdata.dmp.com:2181,node7.zk.bigdata.dmp.com:2181,node8.zk.bigdata.dmp.com:2181,node9.zk.bigdata.dmp.com:2181,node10.zk.bigdata.dmp.com:2181"
        }
    }

    def load(): Unit = {

        val clusterMode = getClusterMode

        if("${cluster.mode}".equals(clusterMode)){
            this.param = loadFromTest
            return
        }

        ClusterMode.valueOf(clusterMode) match {
            case ClusterMode.test => this.param = loadFromTest
            case ClusterMode.local => this.param = loadFromLocal
            case ClusterMode.prod => this.param = loadFromProd
        }

    }

    def load(clusterMode:String): Unit = {

        ClusterMode.valueOf(clusterMode) match {
            case ClusterMode.test => this.param = loadFromTest
            case ClusterMode.local => this.param = loadFromLocal
            case ClusterMode.prod => this.param = loadFromProd
        }

    }

    def loadFromTest: ParameterTool = ParameterTool.fromPropertiesFile(this.getClass.getClassLoader.getResourceAsStream(TEST_CONF_FILENAME))

    def loadFromLocal: ParameterTool = ParameterTool.fromPropertiesFile(this.getClass.getClassLoader.getResourceAsStream(LOCAL_CONF_FILENAME))

    def loadFromProd: ParameterTool = ParameterTool.fromPropertiesFile(this.getClass.getClassLoader.getResourceAsStream(PROD_CONF_FILENAME))
}

