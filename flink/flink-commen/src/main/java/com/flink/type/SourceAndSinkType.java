package com.flink.type;

/**
 * Source、Sink的类型
 *
 * @author sunjingliang
 * @date 2020/5/28
 */
public interface SourceAndSinkType {

    String KAFKA_TOPIC = "KAFKA_TOPIC:";

    String KUDU = "DB_TABLE:prod.";

    String HBASE = "HBASE:";

    String REDIS = "REDIS:";

    String NOT_SINK_REDIS = "||Sink:REDIS:";

    String DRUID = "DRUID:";

    String AEROSPIKE = "||Sink:AEROSPIKE:";

    // 神策
    String SENSOR = "SENSOR:";

    String ELASTICSEARCH = "ELASTICSEARCH:";

    String ALI_RECOMMEND = "||Sink:ALI_RECOMMEND:";
}