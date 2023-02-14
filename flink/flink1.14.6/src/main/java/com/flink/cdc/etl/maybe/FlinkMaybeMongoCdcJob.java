package com.flink.cdc.etl.maybe;

import com.flink.cdc.conf.AppConf;
import com.flink.cdc.deserializer.MongoRecordDeserializer;
import com.flink.cdc.function.MongoCdc2KuduMap;
import com.flink.sink.kudu.conf.KuduOption;
import com.flink.sink.kudu.sink.KuduSink;
import com.flink.sink.kudu.type.KuduOperatorType;
import com.flink.type.SourceAndSinkType;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.concurrent.TimeUnit;

/**
 * description: FlinkMongoCdcJob <br>
 * date: 2022-11-09 17:55 <br>
 * author: YQ <br>
 * version: 1.0 <br>
 */
public class FlinkMaybeMongoCdcJob {

    private static final String KUDU_TABLE = "ods_kudu_mongo_cdc_event_1d";
    private static AppConf appConf = new AppConf();

    public static void main(String[] args) throws Exception {
        AppConf appConf = new AppConf();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(3));
        env.setParallelism(1);
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.setMinPauseBetweenCheckpoints(50000L);
        checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(5));
        checkpointConf.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        SourceFunction<String> mongoSource = MongoDBSource.<String>builder()
                .hosts(appConf.getMaybeMongoHost())
                .username(appConf.getMongoCdcUser())
                .password(appConf.getMongoCdcPassword())
                .copyExisting(false)
                .databaseList("maybe") // set captured database, support regex
                .heartbeatIntervalMillis(60000)
                .deserializer(new MongoRecordDeserializer())
                .build();

        env.addSource(mongoSource)
                .name("maybe-mongo-cdc-source")
                .disableChaining()
                .map(new MongoCdc2KuduMap())
                .name("cdc-convert-to-kudu")
                .addSink(buildKuduSink())
                .name(SourceAndSinkType.KUDU + KUDU_TABLE)
                ;

        env.execute("maybe-mongo-cdc-job");
    }
    private static KuduSink buildKuduSink() {
        KuduOption option = new KuduOption()
                .setMasterAddressList(appConf.getKuduMaster())
                .setMaxWorkerThreadNum(3)
                .setOperationTimeoutMs(120000)
                .setSocketReadTimeoutMs(120000)
                .setBatchSize(8000)
                .setMutationBufferSpaceSize(100000)
                .setTimeoutMs(120000);
        return new KuduSink(KUDU_TABLE, KuduOperatorType.UPSERT_MODE, option);
    }
}
