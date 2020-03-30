package sellcourse.controller


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sellcourse.service.DwdSellCourseService
import util.HiveUtil


object DwdSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_sellcourse_import")
    //.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")

    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    DwdSellCourseService.importCoursePay2(ssc, sparkSession)
    DwdSellCourseService.importCourseShoppingCart2(ssc, sparkSession)
  }
}
