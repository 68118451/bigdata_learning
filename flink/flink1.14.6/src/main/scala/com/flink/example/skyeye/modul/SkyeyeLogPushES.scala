package com.flink.example.skyeye.modul

import scala.beans.BeanProperty

//使用@BeanProperty格式来让样例类可以序列化
//加上BeanProperty的目的是让这个类有getter和setter方法
case class SkyeyeLogPushES(@BeanProperty event_id: String,
                           @BeanProperty app_key: String,
                           @BeanProperty application: String,
                           @BeanProperty app_name: String,
                           @BeanProperty platform: String,
                           @BeanProperty module: String,
                           @BeanProperty level: String,
                           @BeanProperty title: String,
                           @BeanProperty content: String,
                           @BeanProperty device_id: String,
                           @BeanProperty model: String,
                           @BeanProperty app_version: String,
                           @BeanProperty app_ver_code: String,
                           @BeanProperty sdk_version: String,
                           @BeanProperty os: String,
                           @BeanProperty os_version: String,
                           @BeanProperty r: String,
                           @BeanProperty network: String,
                           @BeanProperty sp: String,
                           @BeanProperty city: String,
                           @BeanProperty channel: String,
                           @BeanProperty pkg: String,
                           @BeanProperty imei: String,
                           @BeanProperty oaid: String,
                           @BeanProperty dfid: String,
                           @BeanProperty idfa: String,
                           @BeanProperty uid: String,
                           @BeanProperty event_time: Long
                          )
