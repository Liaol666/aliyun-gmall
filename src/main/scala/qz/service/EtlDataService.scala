package qz.service

import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import qz.bean.{DwdQzPaperView, DwdQzPoint, DwdQzQuestion}
import util.ParseJsonData

object EtlDataService {
  /**
    * 解析章节数据
    *
    * @param ssc
    * @param sparkSession
    * @return
    */
  def etlQzChapter(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzChapter.log").filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions { partition => {
      partition.map(item => {
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        val chapterid: Int = jsonObject.getIntValue("chapterid")
        val chapterlistid: Int = jsonObject.getIntValue("chapterlistid")
        val chaptername: String = jsonObject.getString("chaptername")
        val sequence: String = jsonObject.getString("sequence")
        val showstatus: String = jsonObject.getString("showstatus")
        val status: String = jsonObject.getString("status")
        val creator: String = jsonObject.getString("creator")
        val createtime: String = jsonObject.getString("createtime")
        val courseid: String = jsonObject.getString("courseid")
        val chapternum: String = jsonObject.getString("chapternum")
        val outchapterid: String = jsonObject.getString("outchapterid")
        val dt: String = jsonObject.getString("dt")
        val dn: String = jsonObject.getString("dn")
        (chapterid, chapterlistid, chaptername, sequence, showstatus, creator, createtime,
          courseid, chapternum, outchapterid, dt, dn)
      })
    }
    }.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter")
  }

  /**
    * 解析章节列表数据
    *
    * @param ssc
    * @param sparkSession
    */

  def etlQzChapterList(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzChapterList.log").filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        val chapterlistid: String = jsonObject.getString("chapterlistid")
        val chapterlistname: String = jsonObject.getString("chapterlistname")
        val courseid: String = jsonObject.getString("courseid")
        val chapterallnum: String = jsonObject.getString("chapterallnum")
        val sequence: String = jsonObject.getString("sequence")
        val status: String = jsonObject.getString("status")
        val creator: String = jsonObject.getString("creator")
        val createtime: String = jsonObject.getString("createtime")
        val dt: String = jsonObject.getString("dt")
        val dn: String = jsonObject.getString("dn")
        (chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status, creator, createtime, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter_list")
  }

  /**
    * 解析做题数据
    *
    */
  def etlQzPoint(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzPoint.log").filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val js: JSONObject = ParseJsonData.getJsonData(item)
        val pointid: Int = js.getIntValue("pointid")
        val courseid: Int = js.getIntValue("courseid")
        val pointname: String = js.getString("pointname")
        val pointyear: String = js.getString("pointyear")
        val chapter: String = js.getString("chapter")
        val creator: String = js.getString("creator")
        val createtime: String = js.getString("createtime")
        val status: String = js.getString("status")
        val modifystatus: String = js.getString("modifystatus")
        val excisenum: Int = js.getIntValue("excisenum")
        val pointlistid: Int = js.getIntValue("pointlistid")
        val chapterid: Int = js.getIntValue("chapterid")
        val sequece: String = js.getString("sequece")
        val pointdescribe: String = js.getString("pointdescribe")
        val pointlevel: String = js.getString("pointlevel")
        val typelist: String = js.getString("typelist")
        //使用BigDecimal函数对分数进行四舍五入，1为保留1位小数点，HALF_UP为四舍五入
        val score: BigDecimal = BigDecimal(js.getString("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val thought: String = js.getString("thought")
        val remid: String = js.getString("remid")
        val pointnamelist: String = js.getString("pointnamelist")
        val typelistids: String = js.getString("typelistids")
        val pointlist: String = js.getString("pointlist")
        val dt: String = js.getString("dt")
        val dn: String = js.getString("dn")
        DwdQzPoint(pointid, courseid, pointname, pointyear, chapter, creator, createtime, status, modifystatus, excisenum, pointlistid,
          chapterid, sequece, pointdescribe, pointlevel, typelist, score, thought, remid, pointnamelist, typelistids,
          pointlist, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point")
  }

  /**
    * 解析知识点下的题数据
    *
    * @return
    */
  def etlQzPointQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzPointQuestion.log").filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        val pointid: String = jsonObject.getString("pointid")
        val questionid: String = jsonObject.getString("questionid")
        val questype: String = jsonObject.getString("questype")
        val creator: String = jsonObject.getString("creator")
        val createtime: String = jsonObject.getString("createtime")
        val dt: String = jsonObject.getString("dt")
        val dn: String = jsonObject.getString("dn")
        (pointid, questionid, questype, creator, createtime, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point_question")
  }

  /**
    * 解析网站课程
    *
    */
  def etlQzSiteCourse(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzSiteCourse.log").filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition =>
    partition.map({item =>
      val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
      val sitecourseid: Int = jsonObject.getIntValue("sitecourseid")
      val siteid: Int = jsonObject.getIntValue("siteid")
      val courseid: Int = jsonObject.getIntValue("courseid")
      val sitecoursename: String = jsonObject.getString("sitecoursename")
      val coursechapter: String = jsonObject.getString("coursechapter")
      val sequence: String = jsonObject.getString("sequence")
      val status: String = jsonObject.getString("status")
      val creator: String = jsonObject.getString("creator")
      val createtime: String = jsonObject.getString("createtime")
      val helppaperstatus: String = jsonObject.getString("helppaperstatus")
      val servertype: String = jsonObject.getString("servertype")
      val boardid: Int = jsonObject.getIntValue("boardid")
      val showstatus: String = jsonObject.getString("showstatus")
      val dt: String = jsonObject.getString("dt")
      val dn: String = jsonObject.getString("dn")
      (sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status, creator
        , createtime, helppaperstatus, servertype, boardid, showstatus, dt, dn)
    })).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_site_course")
  }

  /**
    * 解析课程数据
    *
    */
  def etlQzCourse(ssc: SparkContext, sparkSession: SparkSession)={
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzCourse.log").filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition=>{
      partition.map(item => {
        val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
        val courseid: Int = jSONObject.getIntValue("courseid")
        val majorid: Int = jSONObject.getIntValue("majorid")
        val coursename: String = jSONObject.getString("coursename")
        val coursechapter: String = jSONObject.getString("coursechapter")
        val sequence: String = jSONObject.getString("sequnece")
        val isadvc: String = jSONObject.getString("isadvc")
        val creator: String = jSONObject.getString("creator")
        val createtime: String = jSONObject.getString("createtime")
        val status: String = jSONObject.getString("status")
        val chapterlistid: Int = jSONObject.getIntValue("chapterlistid")
        val pointlistid: Int = jSONObject.getIntValue("pointlistid")
        val dt: String = jSONObject.getString("dt")
        val dn: String = jSONObject.getString("dn")
        (courseid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime, status
          , chapterlistid, pointlistid, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course")
  }

  /**
    * 解析课程辅导数据
    *
    */
  def etlQzCourseEdusubject(ssc: SparkContext, sparkSession: SparkSession)={
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzCourseEduSubject.log").filter(item=>{
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
        val courseeduid: Int = jSONObject.getIntValue("courseeduid")
        val edusubjectid: Int = jSONObject.getIntValue("edusubjectid")
        val courseid: Int = jSONObject.getIntValue("courseid")
        val creator: String = jSONObject.getString("creator")
        val createtime: String = jSONObject.getString("createtime")
        val majorid: Int = jSONObject.getIntValue("majorid")
        val dt: String = jSONObject.getString("dt")
        val dn: String = jSONObject.getString("dn")
        (courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course_edusubject")
  }
  /**
    * 解析课程网站
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzWebsite(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzWebsite.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val domain = jsonObject.getString("domain")
        val sequence = jsonObject.getString("sequence")
        val multicastserver = jsonObject.getString("multicastserver")
        val templateserver = jsonObject.getString("templateserver")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val multicastgateway = jsonObject.getString("multicastgateway")
        val multicastport = jsonObject.getString("multicastport")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime,
          multicastgateway, multicastport, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_website")
  }

  /**
    * 解析主修数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzMajor(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzMajor.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val majorid = jsonObject.getIntValue("majorid")
        val businessid = jsonObject.getIntValue("businessid")
        val siteid = jsonObject.getIntValue("siteid")
        val majorname = jsonObject.getString("majorname")
        val shortname = jsonObject.getString("shortname")
        val status = jsonObject.getString("status")
        val sequence = jsonObject.getString("sequence")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val columm_sitetype = jsonObject.getString("columm_sitetype")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (majorid, businessid, siteid, majorname, shortname, status, sequence, creator, createtime, columm_sitetype, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_major")
  }

  /**
    * 解析做题业务
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzBusiness(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzBusiness.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item);
        val businessid = jsonObject.getIntValue("businessid")
        val businessname = jsonObject.getString("businessname")
        val sequence = jsonObject.getString("sequence")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val siteid = jsonObject.getIntValue("siteid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_business")
  }

  def etlQzPaperView(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzPaperView.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val paperid = jsonObject.getIntValue("paperid")
        val paperviewname = jsonObject.getString("paperviewname")
        val paperparam = jsonObject.getString("paperparam")
        val openstatus = jsonObject.getString("openstatus")
        val explainurl = jsonObject.getString("explainurl")
        val iscontest = jsonObject.getString("iscontest")
        val contesttime = jsonObject.getString("contesttime")
        val conteststarttime = jsonObject.getString("conteststarttime")
        val contestendtime = jsonObject.getString("contestendtime")
        val contesttimelimit = jsonObject.getString("contesttimelimit")
        val dayiid = jsonObject.getIntValue("dayiid")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val paperviewcatid = jsonObject.getIntValue("paperviewcatid")
        val modifystatus = jsonObject.getString("modifystatus")
        val description = jsonObject.getString("description")
        val papertype = jsonObject.getString("papertype")
        val downurl = jsonObject.getString("downurl")
        val paperuse = jsonObject.getString("paperuse")
        val paperdifficult = jsonObject.getString("paperdifficult")
        val testreport = jsonObject.getString("testreport")
        val paperuseshow = jsonObject.getString("paperuseshow")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        DwdQzPaperView(paperviewid, paperid, paperviewname, paperparam, openstatus, explainurl, iscontest, contesttime,
          conteststarttime, contestendtime, contesttimelimit, dayiid, status, creator, createtime, paperviewcatid, modifystatus,
          description, papertype, downurl, paperuse, paperdifficult, testreport, paperuseshow, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper_view")
  }

  def etlQzCenterPaper(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzCenterPaper.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val centerid = jsonObject.getIntValue("centerid")
        val openstatus = jsonObject.getString("openstatus")
        val sequence = jsonObject.getString("sequence")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center_paper")
  }

  def etlQzPaper(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzPaper.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val paperid = jsonObject.getIntValue("paperid")
        val papercatid = jsonObject.getIntValue("papercatid")
        val courseid = jsonObject.getIntValue("courseid")
        val paperyear = jsonObject.getString("paperyear")
        val chapter = jsonObject.getString("chapter")
        val suitnum = jsonObject.getString("suitnum")
        val papername = jsonObject.getString("papername")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val craetetime = jsonObject.getString("createtime")
        val totalscore = BigDecimal.apply(jsonObject.getString("totalscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val chapterid = jsonObject.getIntValue("chapterid")
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator, craetetime, totalscore, chapterid,
          chapterlistid, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper")
  }

  def etlQzCenter(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzCenter.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(parititons => {
      parititons.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val centerid = jsonObject.getIntValue("centerid")
        val centername = jsonObject.getString("centername")
        val centeryear = jsonObject.getString("centeryear")
        val centertype = jsonObject.getString("centertype")
        val openstatus = jsonObject.getString("openstatus")
        val centerparam = jsonObject.getString("centerparam")
        val description = jsonObject.getString("description")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val sequence = jsonObject.getString("sequence")
        val provideuser = jsonObject.getString("provideuser")
        val centerviewtype = jsonObject.getString("centerviewtype")
        val stage = jsonObject.getString("stage")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime,
          sequence, provideuser, centerviewtype, stage, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center")
  }

  def etlQzQuestion(ssc: SparkContext, sparkSession: SparkSession)={
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzQuestion.log").filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
        val questionid: Int = jSONObject.getIntValue("questionid")
        val parentid: Int = jSONObject.getIntValue("parentid")
        val questypeid: Int = jSONObject.getIntValue("questypeid")
        val quesviewtype: Int = jSONObject.getIntValue("quesviewtype")
        val content: String = jSONObject.getString("content")
        val answer: String = jSONObject.getString("answer")
        val analysis: String = jSONObject.getString("analysis")
        val limitminute: String = jSONObject.getString("limitminute")
        val score: BigDecimal = BigDecimal.apply(jSONObject.getDoubleValue("score")).setScale(1,BigDecimal.RoundingMode.HALF_UP)
        val splitscore: BigDecimal = BigDecimal.apply(jSONObject.getDoubleValue("splitscore")).setScale(1,BigDecimal.RoundingMode.HALF_UP)
        val status: String = jSONObject.getString("status")
        val optnum: Int = jSONObject.getIntValue("optnum")
        val lecture: String = jSONObject.getString("lecture")
        val creator: String = jSONObject.getString("creator")
        val createtime: String = jSONObject.getString("createtime")
        val modifystatus: String = jSONObject.getString("modifystatus")
        val attanswer: String = jSONObject.getString("attanswer")
        val questag: String = jSONObject.getString("questag")
        val vanalysisaddr: String = jSONObject.getString("vanalysisaddr")
        val difficulty: String = jSONObject.getString("difficulty")
        val quesskill: String = jSONObject.getString("quesskill")
        val vdeoaddr: String = jSONObject.getString("vdeoaddr")
        val dt: String = jSONObject.getString("dt")
        val dn: String = jSONObject.getString("dn")
        DwdQzQuestion(questionid, parentid, questypeid, quesviewtype, content, answer, analysis, limitminute, score, splitscore,
          status, optnum, lecture, creator, createtime, modifystatus, attanswer, questag, vanalysisaddr, difficulty, quesskill,
          vdeoaddr, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question")
  }

  def etlQzQuestionType(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzQuestionType.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val quesviewtype = jsonObject.getIntValue("quesviewtype")
        val viewtypename = jsonObject.getString("viewtypename")
        val questiontypeid = jsonObject.getIntValue("questypeid")
        val description = jsonObject.getString("description")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val papertypename = jsonObject.getString("papertypename")
        val sequence = jsonObject.getString("sequence")
        val remark = jsonObject.getString("remark")
        val splitscoretype = jsonObject.getString("splitscoretype")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (quesviewtype, viewtypename, questiontypeid, description, status, creator, createtime, papertypename, sequence,
          remark, splitscoretype, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question_type")
  }


  /**
    * 解析用户做题情况数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzMemberPaperQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/zhang/ods/QzMemberPaperQuestion.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val userid = jsonObject.getIntValue("userid")
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val chapterid = jsonObject.getIntValue("chapterid")
        val sitecourseid = jsonObject.getIntValue("sitecourseid")
        val questionid = jsonObject.getIntValue("questionid")
        val majorid = jsonObject.getIntValue("majorid")
        val useranswer = jsonObject.getString("useranswer")
        val istrue = jsonObject.getString("istrue")
        val lasttime = jsonObject.getString("lasttime")
        val opertype = jsonObject.getString("opertype")
        val paperid = jsonObject.getIntValue("paperid")
        val spendtime = jsonObject.getIntValue("spendtime")
        val score = BigDecimal.apply(jsonObject.getString("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val question_answer = jsonObject.getIntValue("question_answer")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (userid, paperviewid, chapterid, sitecourseid, questionid, majorid, useranswer, istrue, lasttime, opertype, paperid, spendtime, score, question_answer, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_member_paper_question")
  }

}
