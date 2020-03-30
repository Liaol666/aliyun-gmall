package member.controller

import member.service.EtlDataService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import util.HiveUtil

object DwdMemberController {
  def main(args: Array[String]): Unit = {
    //这个root是说明要引入的hdfs的数据的owner是root，如果不写，会没有权限读取
    System.setProperty("HAOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setAppName("dwd_member_import")
    //spark需要使用hive所以需要提供hive支持
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    //配置高可用地址，解析日志的路径就不用填写全名称
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    //    HiveUtil.useSnappyCompression(sparkSession)//使用snappy压缩
    //对用户原始数据进行数据清洗，存入bd1层表中
    EtlDataService.etlBaseAdLog(ssc, sparkSession) //导入基础广告数据
    EtlDataService.etlBaseWebSiteLog(ssc, sparkSession) //导入基础网站表数据
    EtlDataService.etlMemberLog(ssc, sparkSession) //清洗用户数据
    EtlDataService.etlMemberRegtypeLog(ssc, sparkSession) //清洗用户注册数据
    EtlDataService.etlMemPayMoneyLog(ssc, sparkSession) //导入用户支付情况记录
    EtlDataService.etlMemVipLevelLog(ssc, sparkSession) //导入vip基础数据
  }
}
