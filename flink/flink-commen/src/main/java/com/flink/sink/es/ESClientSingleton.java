package com.flink.sink.es;


import suishen.elasticsearch.rest.client.RestHighLevelClient;
import suishen.elasticsearch.rest.client.config.HttpHostConfig;

public class ESClientSingleton {
    private volatile static RestHighLevelClient single = null;

    // 私有构造
    private ESClientSingleton() {}
    // 双重检查
    public static RestHighLevelClient getInstance(java.util.List<HttpHostConfig> esClusterConfigs) throws Exception {
        if (single == null) {
            synchronized (ESClientSingleton.class) {
                if (single == null) {
                    single = new RestHighLevelClient(esClusterConfigs);
                }
            }
        }
        return single;
    }
}
