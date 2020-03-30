package sellcourse.service

import java.sql.Timestamp

import org.apache.spark.sql._
import sellcourse.bean.{DwdCourseShoppingCart, DwdSaleCourse}
import sellcourse.dao.DwdSellCourseDao

import scala.collection.immutable.StringOps
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DwsSellCourseService {

  def importSellCourseDetail(sparkSession: SparkSession, dt: String) = {
    val dwdSaleCourse = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt='${dt}'")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    dwdSaleCourse.join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  def importSellCourseDetail2(sparkSession: SparkSession, dt: String) = {
    //解决数据倾斜，将小表进行扩容 大表key加上随机散列值，这个可以解决数据倾斜，但是不能起到优化的作用，耗时或许比原来更久
    val dwdSaleCourse: Dataset[Row] = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt=${dt}")
    val dwdCourseShoppingCart: Dataset[Row] = DwdSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay: Dataset[Row] = DwdSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime").coalesce(1)
    import sparkSession.implicits._
    //先给大表的k加一个带前缀的列
    val newdwdCourseShoppingCart: Dataset[DwdCourseShoppingCart] = dwdCourseShoppingCart.mapPartitions(partition => {
      partition.map(item => {
        val courseid: Int = item.getAs[Int]("courseid")
        val random: Int = Random.nextInt(100)
        DwdCourseShoppingCart(courseid, item.getAs[String]("orderid"),
          item.getAs[String]("coursename"), item.getAs[java.math.BigDecimal]("cart_discount"),
          item.getAs[java.math.BigDecimal]("sellmoney"), item.getAs[Timestamp]("cart_createtime"),
          item.getAs[String]("dt"), item.getAs[String]("dn"), courseid + "_" + random)
      })
    })

    //对小表扩充100倍
    val newdwdSaleCourse: Dataset[DwdSaleCourse] = dwdSaleCourse.flatMap(item => {
      val list = new ArrayBuffer[DwdSaleCourse]()
      val courseid: Int = item.getAs[Int]("courseid")
      val coursename: StringOps = item.getAs[String]("coursename")
      val status: StringOps = item.getAs[String]("status")
      val pointlistid: Int = item.getAs[Int]("pointlistid")
      val majorid: Int = item.getAs[Int]("majorid")
      val chapterid: Int = item.getAs[Int]("chapterid")
      val chaptername: StringOps = item.getAs[String]("chaptername")
      val edusubjectid: Int = item.getAs[Int]("edusubjectid")
      val edusubjectname: StringOps = item.getAs[String]("edusubjectname")
      val teacherid: Int = item.getAs[Int]("teacherid")
      val teachername: StringOps = item.getAs[String]("teachername")
      val coursemanager: StringOps = item.getAs[String]("coursemanager")
      val money: java.math.BigDecimal = item.getAs[java.math.BigDecimal]("money")
      val dt: StringOps = item.getAs[String]("dt")
      val dn: StringOps = item.getAs[String]("dn")
      for (i <- 0 until 100) {
        list.append(DwdSaleCourse(courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
          edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, courseid + "_" + i))
      }
      list.toIterator
    })
    //小表和大表join
    newdwdSaleCourse.join(newdwdCourseShoppingCart.drop("courseid").drop("coursename"),
      //因为小表在左，大表在右，所以使用right，这样遍历小表join大表
      Seq("rand_courseid", "dt", "dn"), "right")
      //大表在左小表在右，所以left
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  def importSellCourseDetail3(sparkSession: SparkSession, dt: String) = {
    //最常用的解决数据倾斜的问题，采用广播小表
    val dwdSaleCourse: Dataset[Row] = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt=${dt}")
    val dwdCourseShoppingCart: Dataset[Row] = DwdSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt=${dt}")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay: Dataset[Row] = DwdSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import org.apache.spark.sql.functions._
    //广播小表，join大表，在本地join，避免网络传输造成的消耗,同时大表在右，使用right
    broadcast(dwdSaleCourse).join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  def importSellCourseDetail4(sparkSession: SparkSession, dt: String) = {
    //解决数据倾斜问题 采用广播小表
    val dwdSaleCourse: Dataset[Row] = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt=${dt}")
    val dwdCourseShoppingCart: Dataset[Row] = DwdSellCourseDao.getDwdCourseShoppingCart2(sparkSession).where(s"dt=${dt}")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay: Dataset[Row] = DwdSellCourseDao.getDwdCoursePay2(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import org.apache.spark.sql.functions._
    val tmpTable: DataFrame = dwdCourseShoppingCart.join(dwdCoursePay, Seq("orderid"), "left")
    broadcast(dwdSaleCourse).join(tmpTable, Seq("courseid"), "right")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dwd.dwd_sale_course.dt", "dwd.dwd_sale_course.dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }
}
