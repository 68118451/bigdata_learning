package com.flink.serialize;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.lang.reflect.Method;

/**
 * @author fanrui
 * @time 2019-12-12 21:11:31
 */
public class ProtobufDeserialize<T extends com.google.protobuf.GeneratedMessage>
        extends AbstractDeserializationSchema<T> {

    private Logger LOG = LoggerFactory.getLogger(ProtobufDeserialize.class);

    /**
     * protobuf 的 parseFrom 方法
      */
    private transient Method parseFrom;

    public ProtobufDeserialize(Class<T> type) {
        super(type);
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            if(parseFrom == null){
                // 获取 protobuf 的 parseFrom 方法
                parseFrom = getProducedType().getTypeClass().getMethod("parseFrom", byte[].class);
            }
            return (T)parseFrom.invoke(null, message);
        } catch (Exception e) {
            LOG.error("Protobuf deserialize failed.", e);
            throw new InvalidObjectException("protobuf 解析异常");
        }
    }
}
