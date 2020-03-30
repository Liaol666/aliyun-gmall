package member.controller

import member.service.AdsMemberService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import util.HiveUtil

object AdsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setAppName("ads_member_controller").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession)//开启动态分区
    AdsMemberService.queryDetailApi(sparkSession, "20190722")
  }

}
