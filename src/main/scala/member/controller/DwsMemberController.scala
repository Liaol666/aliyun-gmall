package member.controller

import member.service.DwsMemberService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import util.HiveUtil

object DwsMemberController {
  def main(args: Array[String]): Unit = {
    //设置读取的用户，owner是root需要root权限
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setAppName("dws_member_import").setMaster("local[*]")
//      .set("spark.sql.autoBroadcastJoinThreshold","104857600")//104857600==100Mb
      .set("spark.sql.shuffle.partitions","30")
    //创建sparkSession以及提供hive支持1
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    //配置高可用地址，解析日志的路径就不用填写全名称
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession)//开启动态分区
    HiveUtil.openCompression(sparkSession)//开启压缩
//    HiveUtil.useSnappyCompression(sparkSession) //开启snappy
    DwsMemberService.importMember(sparkSession,"20190722")
    DwsMemberService.importMemberUseApi(sparkSession,"20190722")
  }
}
