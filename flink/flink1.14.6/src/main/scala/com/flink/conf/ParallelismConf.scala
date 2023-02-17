package com.flink.conf

import org.slf4j.LoggerFactory

/**
  * @author gaojingda
  *         Description: 根据是否是测试环境设置并行度
  * @date 2019-07-17
  */
object ParallelismConf {

    private val LOG = LoggerFactory.getLogger(getClass)

    val appConf = AppConf()

    def setParallelism(p: Int) : Int = {

        var param = p
        try {
            param = appConf.getClusterGlobalParallelism(p)
        } catch {
            case e: Exception => {
                LOG.error("测试集群配置存在异常， 强制改为线上集群配置", e)
                param = p
            }
        }
        param
    }

}
