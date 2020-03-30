package qz.controller

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import qz.service.AdsQzService
import util.HiveUtil

object AdsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf: SparkConf = new SparkConf().setAppName("ads_member_controller")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession)
    AdsQzService.getTargetApi(sparkSession,"20190722")
  }
}
