package com.flink.scala.core


//导入隐式转换

import org.apache.flink.streaming.api.scala._

object WordCount {


  def main(args: Array[String]): Unit = {


    /**
      * 创建flink环境
      *
      */

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    //默认并行度由系统核数决定
//    env.setParallelism(2)

    //读取socket 构建DS
    //nc -lk 8888
    val linesDS: DataStream[String] = env.socketTextStream("node2.azkaban.bigdata.hw.com",8888)


    //1、将单词拆分
    val wordDS: DataStream[String] = linesDS.flatMap(_.split(","))


    //2、转换成kv格式
    val kvDS: DataStream[(String, Int)] = wordDS.map((_, 1))


    //3、按照key进行分组， 底层也是hash分区
    val keyByDS: KeyedStream[(String, Int), String] = kvDS.keyBy(_._1)

    /**
      * sum算子内部是有状态计算，累加统计
      */
    //4、对value进行聚合
    val countDS: DataStream[(String, Int)] = keyByDS.sum(1)


    //打印结果
    countDS.print()


    //启动flink程序
    env.execute()


  }

}
