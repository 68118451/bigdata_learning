package com.flink.java.core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // 机器启动 nc -lk 8888
        // 没有nc 需要 yum -y install nc安装

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("node2.azkaban.bigdata.hw.com", 8888);

        streamSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {

                        //对每一行数据按','切分，切分后的每一个单词输出(word,1)二元组
                        for(String word: s.split(",")){

                            out.collect(new Tuple2<String, Integer>(word, 1));

                        }

                    }
                })
                .keyBy(value->value.f0)
                .sum(1)
                .print();

        env.execute("WordCount");



    }
}

