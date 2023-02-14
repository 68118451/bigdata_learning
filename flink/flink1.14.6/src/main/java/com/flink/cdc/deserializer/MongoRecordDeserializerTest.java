package com.flink.cdc.deserializer;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which deserializes the
 * received {@link SourceRecord} to JSON String.
 */
public class MongoRecordDeserializerTest implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 2L;



    public MongoRecordDeserializerTest() {
    }


    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
         out.collect(record.value().toString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
