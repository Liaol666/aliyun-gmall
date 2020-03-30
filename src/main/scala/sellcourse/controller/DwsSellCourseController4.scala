package sellcourse.controller


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sellcourse.service.DwsSellCourseService
import util.HiveUtil

object DwsSellCourseController4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_sellcourse_import")
//      .set("spark.sql.autoBroadcastJoinThreshold", "1")
//      .set("spark.sql.shuffle.partitions", "12")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    DwsSellCourseService.importSellCourseDetail2(sparkSession, "20190722")
  }
}
