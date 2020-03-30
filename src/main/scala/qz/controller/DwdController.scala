package qz.controller

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import qz.service.EtlDataService
import util.HiveUtil

object DwdController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf: SparkConf = new SparkConf().setAppName("dwd_qz_controller")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openCompression(sparkSession)
    HiveUtil.openDynamicPartition(sparkSession)//开启动态分区
    EtlDataService.etlQzChapter(ssc, sparkSession)
    EtlDataService.etlQzChapterList(ssc, sparkSession)
    EtlDataService.etlQzPoint(ssc, sparkSession)
    EtlDataService.etlQzPointQuestion(ssc, sparkSession)
    EtlDataService.etlQzSiteCourse(ssc, sparkSession)
    EtlDataService.etlQzCourse(ssc, sparkSession)
    EtlDataService.etlQzCourseEdusubject(ssc, sparkSession)
    EtlDataService.etlQzWebsite(ssc, sparkSession)
    EtlDataService.etlQzMajor(ssc, sparkSession)
    EtlDataService.etlQzBusiness(ssc, sparkSession)
    EtlDataService.etlQzPaperView(ssc, sparkSession)
    EtlDataService.etlQzCenterPaper(ssc, sparkSession)
    EtlDataService.etlQzPaper(ssc, sparkSession)
    EtlDataService.etlQzCenter(ssc, sparkSession)
    EtlDataService.etlQzQuestion(ssc, sparkSession)
    EtlDataService.etlQzQuestionType(ssc, sparkSession)
    EtlDataService.etlQzMemberPaperQuestion(ssc, sparkSession)
  }
}
