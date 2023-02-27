package com.flink.example.skyeye.function

import com.flink.example.skyeye.SkyeyeLogEventToES.{PASSWORD, URL, USER, getAppMap}
import com.flink.example.skyeye.modul.{SkyeyeLog, SkyeyeLogPushES}
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.mutable


class SkyeyeLogEventMap extends MapFunction[SkyeyeLog.Entity, SkyeyeLogPushES] {

  var appMap: mutable.HashMap[String, (String, String, String, String)] = new mutable.HashMap[String, (String, String, String, String)]


  def this(appmap: mutable.HashMap[String, (String, String, String, String)]) {
    this()
    this.appMap = appmap
  }


  override def map(row: SkyeyeLog.Entity): SkyeyeLogPushES = {

    //获取数据中的app_key
    val app_key: String = row.getAppKey

    var application: String = ""
    var app_name: String = ""
    var platform: String = ""
    //根据app_key来与mysql查询出来的数据对应获取application、app_naem和platform
    if (appMap.contains(app_key)) {
      application = appMap.get(app_key).getOrElse(("","","",""))._1
      app_name = appMap.get(app_key).getOrElse(("","","",""))._2
      platform = appMap.get(app_key).getOrElse(("","","",""))._3
      }else{
      //如果app_key获取不到对应的数据则更新appMap
      appMap = getAppMap(URL, USER, PASSWORD)
      application = appMap.get(app_key).getOrElse(("","","",""))._1
      app_name = appMap.get(app_key).getOrElse(("","","",""))._2
      platform = appMap.get(app_key).getOrElse(("","","",""))._3
    }


    //将数据封装成SkyeyeLogPushES类型
    SkyeyeLogPushES(row.getEventId,
      app_key,
      application,
      app_name,
      platform,
      row.getModule,
      row.getLevel,
      row.getTitle,
      row.getContent,
      row.getDeviceId,
      row.getModel,
      row.getAppVersion,
      row.getAppVerCode.toString,
      row.getSdkVersion,
      row.getOs,
      row.getOsVersion,
      row.getR,
      row.getNetwork,
      row.getSp,
      row.getCity,
      row.getChannel,
      row.getPkg,
      row.getImei,
      row.getOaid,
      row.getDfId,
      row.getIdfa,
      row.getUid,
      row.getEventTime
    )


  }
}
