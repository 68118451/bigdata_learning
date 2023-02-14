package com.flink.cdc.etl.peanut;

import com.alibaba.fastjson.JSONObject;
import com.flink.cdc.conf.AppConf;
import com.flink.cdc.deserializer.MongoToKafkaDeserializer;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;


import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * flink cdc 实现同一mongo数据源写入kafka topic供下游使用,下游消费kafka类型debezium-json
 * 作者：
 * 日期：2022/12/20 下午5:21
 */

public class FlinkPeanutMongoToKafka {

    private static final String KAFKA_PRODUCER_MONGO_CDC_TOPIC = "peanut_mongo_to_kafka";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node2.kafka.bigdata.hw.com:9092,node5.kafka.bigdata.hw.com:9092,node6.kafka.bigdata.hw.com:9092";


    public static void main(String[] args) throws Exception {

        AppConf appConf = new AppConf();
//        1.构建flink环境及配置checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(5));
        env.setParallelism(1);
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.setMinPauseBetweenCheckpoints(50000L);
        checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(5));
        checkpointConf.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //这里使用SourceFunction而不是最新的MongoDBSource(cdc2.3.0后的新方法,但是当集合改名或者删除时会报错)
        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
                .hosts(appConf.getPeanutMongoHost())
                .username(appConf.getMongoCdcUser())
                .password(appConf.getMongoCdcPassword())
                .copyExisting(false)
                .databaseList("peanut") // set captured database, support regex
                .deserializer(new MongoToKafkaDeserializer())
                .build();


        DataStreamSource<String> streamSource = env.addSource(sourceFunction);


        streamSource.sinkTo(getKafkaProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_PRODUCER_MONGO_CDC_TOPIC, "key"));

        env.execute("flink-peanut-mongo-to-kafka");


    }


    public static KafkaSink<String> getKafkaProducer(String brokers, String topic, String filed) {
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "1");


        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        // 设置分区字段
                        .setPartitioner(new FlinkKafkaPartitioner<String>() {
                            @Override
                            public int partition(String record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                                JSONObject jsonObject = JSONObject.parseObject(record);
                                Object source = jsonObject.get("source");
                                JSONObject sourceJson = JSONObject.parseObject(JSONObject.toJSONString(source));
                                Object o = "";
                                if (sourceJson.get(filed) != null){
                                    o = sourceJson.get(filed);
                                }
                                return Math.abs(o.hashCode() % partitions.length);
                            }
                        })
                        .build()
                )
                //setDeliverGuarantee 1.14官方文档有错误,1.15修改过来了
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();


    }
}
