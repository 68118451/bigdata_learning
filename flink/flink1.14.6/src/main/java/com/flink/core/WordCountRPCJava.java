package com.flink.core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountRPCJava {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createRemoteEnvironment("10.112.21.155", 38019, "D:\\IdeaProjects2\\bigdata_learning\\flink\\flink1.14.6\\target\\flink1.14.6.jar");


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


        env.execute("WordCountRPC");

    }
}
