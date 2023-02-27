package com.flink.example.skyeye.modul

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty}
import com.suishen.libs.meta.Bean

import java.util.Objects


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class SkyeyeLogESMapper extends Bean {

  @JsonProperty(value = "event_id")
  private var eventId: String = _

  @JsonProperty(value = "app_key")
  private var appKey: String = _

  @JsonProperty(value = "application")
  private var application: String = _

  @JsonProperty(value = "module")
  private var module: String = _

  @JsonProperty(value = "level")
  private var level: String = _

  @JsonProperty(value = "title")
  private var title: String = _

  @JsonProperty(value = "content")
  private var content: String = _

  @JsonProperty(value = "app_version")
  private var appVersion: String = _

  @JsonProperty(value = "app_ver_code")
  private var appVersionCode: String = _

  @JsonProperty(value = "sdk_version")
  private var sdkVersion: String = _

  @JsonProperty(value = "os")
  private var os: String = _

  @JsonProperty(value = "os_version")
  private var osVersion: String = _

  @JsonProperty(value = "r")
  private var r: String = _

  @JsonProperty(value = "network")
  private var network: String = _

  @JsonProperty(value = "sp")
  private var sp: String = _

  @JsonProperty(value = "city")
  private var city: String = _

  @JsonProperty(value = "channel")
  private var channel: String = _

  @JsonProperty(value = "imei")
  private var imei: String = _

  @JsonProperty(value = "oaid")
  private var oaid: String = _

  @JsonProperty(value = "dfid")
  private var dfid: String = _

  @JsonProperty(value = "idfa")
  private var idfa: String = _

  @JsonProperty(value = "uid")
  private var uid: String = _

  @JsonProperty(value = "event_time")
  private var eventTime: java.lang.Long = _

  @JsonProperty(value = "platform")
  private var platform: String = _

  @JsonProperty(value = "pkg")
  private var pkg: String = _

  @JsonProperty(value = "device_id")
  private var deviceId: String = _

  @JsonProperty(value = "model")
  private var model: String = _

  @JsonProperty(value = "app_name")
  private var appName: String = _

  def getEventId: String = eventId

  def setEventId(eventId: String): Unit = {
    this.eventId = eventId
  }

  def getAppKey: String = appKey

  def setAppKey(appKey: String): Unit = {
    this.appKey = appKey
  }

  def getApplication: String = application

  def setApplication(application: String): Unit = {
    this.application = application
  }

  def getModule: String = module

  def setModule(module: String): Unit = {
    this.module = module
  }

  def getlevel: String = level

  def setlevel(level: String): Unit = {
    this.level = level
  }


  def getTitle: String = title

  def setTitle(title: String): Unit = {
    this.title = title
  }

  def getContent: String = content

  def setContent(content: String): Unit = {
    this.content = content
  }

  def getAppVersion: String = appVersion

  def setAppVersion(appVersion: String): Unit = {
    this.appVersion = appVersion
  }

  def getAppVersionCode: String = appVersionCode

  def setAppVersionCode(appVersionCode: String): Unit = {
    this.appVersionCode = appVersionCode
  }

  def getSdkVersion: String = sdkVersion

  def setSdkVersion(sdkVersion: String): Unit = {
    this.sdkVersion = sdkVersion
  }

  def getOs: String = os

  def setOs(os: String): Unit = {
    this.os = os
  }

  def getOsVersion: String = osVersion

  def setOsVersion(osVersion: String): Unit = {
    this.osVersion = osVersion
  }

  def getR: String = r

  def setR(r: String): Unit = {
    this.r = r
  }

  def getNetwork: String = network

  def setNetwork(network: String): Unit = {
    this.network = network
  }

  def getSp: String = sp

  def setSp(sp: String): Unit = {
    this.sp = sp
  }

  def getCity: String = city

  def setCity(city: String): Unit = {
    this.city = city
  }

  def getChannel: String = channel

  def setChannel(channel: String): Unit = {
    this.channel = channel
  }

  def getImei: String = imei

  def setImei(imei: String): Unit = {
    this.imei = imei
  }

  def getOaid: String = oaid

  def setOaid(oaid: String): Unit = {
    this.oaid = oaid
  }

  def getDfid: String = dfid

  def setDfid(dfid: String): Unit = {
    this.dfid = dfid
  }

  def getIdfa: String = idfa

  def setIdfa(idfa: String): Unit = {
    this.idfa = idfa
  }

  def getUid: String = uid

  def setUid(uid: String): Unit = {
    this.uid = uid
  }

  def getEventTime: java.lang.Long = eventTime

  def setEventTime(eventTime: java.lang.Long): Unit = {
    this.eventTime = eventTime
  }

  def getPlatform: String = platform

  def setPlatform(platform: String): Unit = {
    this.platform = platform
  }

  def getPkg: String = pkg

  def setPkg(pkg: String): Unit = {
    this.pkg = pkg
  }

  def getDeviceId: String = deviceId

  def setDeviceId(deviceId: String): Unit = {
    this.deviceId = deviceId
  }

  def getModel: String = model

  def setModel(model: String): Unit = {
    this.model = model
  }

  def getAppName: String = appName

  def setAppName(appName: String): Unit = {
    this.appName = appName
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[AnyRef]) return false
    if (this eq o.asInstanceOf[AnyRef]) return true
    if (o == null || (getClass ne o.getClass)) return false
    val that = o.asInstanceOf[SkyeyeLogESMapper]
    Objects.equals(eventId, that.eventId) && Objects.equals(appKey, that.appKey) && Objects.equals(level, that.level) && Objects.equals(title, that.title)
  }

  override def hashCode: Int = Objects.hash(eventId, appKey, level, title)

  override def toString = s"LogESMapper(event_id:$eventId, app_key:$appKey, app_name:$appName, application:$application, module:$module, level:$level, title:$title, content:$content)"


}
