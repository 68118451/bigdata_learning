package com.flink.example.skyeye.function

import com.flink.example.skyeye.SkyeyeLogEventToES.{PASSWORD, URL, USER, getAppMap}
import com.flink.example.skyeye.modul.SkyeyeLogPushES
import org.apache.flink.api.common.functions.FilterFunction

import scala.collection.mutable


class SkyeyeLogEventFilterApp extends FilterFunction[SkyeyeLogPushES]{

  var appMap: mutable.HashMap[String, (String, String, String, String)] = new mutable.HashMap[String, (String, String, String, String)]
  override def filter(value: SkyeyeLogPushES): Boolean = {
    val app_key = value.app_key
    var `type`: String = ""

    //根据app_key来获取对应mysql查询出来的`type`字段
    if (appMap.contains(app_key)) {
      `type` = appMap.get(app_key).getOrElse(("","","",""))._4
    }else{
      //如果app_key获取不到对应的数据则更新appMap
      appMap = getAppMap(URL, USER, PASSWORD)
      `type` = appMap.get(app_key).getOrElse(("","","",""))._4
    }

    //当app_key对应的`type`为0时判断为app
    "0".equals(`type`)
  }

  def this(appmap:mutable.HashMap[String, (String,String,String,String)]){
    this()
    this.appMap = appmap
  }
}
