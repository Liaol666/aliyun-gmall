package member.service

import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import util.ParseJsonData

object EtlDataService {
  //Etl清洗，对采集到的日志进行数据清洗，存入bd
    def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession) = {
      import sparkSession.implicits._
      //隐式转换，没有这个不能.DF()
      ssc.textFile("/user/zhang/ods/memberRegtype.log")
        .filter(item => {
          //调用json格式检验数据
          val obj: JSONObject = ParseJsonData.getJsonData(item)
          //如果是json格式就留下
          obj.isInstanceOf[JSONObject]
        }).mapPartitions {
        //按照区进行map
        partition => {
          partition.map(item => {
            //对每个区的元素进行map
            val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
            val appkey: String = jsonObject.getString("appkey")
            val appregurl: String = jsonObject.getString("appregurl")
            val bdp_uuid: String = jsonObject.getString("bdp_uuid")
            val createtime: String = jsonObject.getString("createtime")
            val isranreg: String = jsonObject.getString("isranreg")
            val regsource: String = jsonObject.getString("regsource")
            val regsourceName: String = regsource match {
              case "1" => "PC"
              case "2" => "Moblie"
              case "3" => "App"
              case "4" => "WeChat"
              case _ => "other"
            }
            val uid: String = jsonObject.getString("uid")
            val websiteid: String = jsonObject.getString("websiteid")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            //如果元素少于22个，可以使用元组，如果对于22个需要使用样例类,并且封装的顺序
            // 要和建表字段的顺序一致
            (uid, appkey, appregurl, bdp_uuid, createtime, isranreg, regsource, regsourceName, websiteid, dt, dn)
          }
          )
        }
        //toDF需要隐式转换之后才能出现,coalesce减少分区，作用是减少hdfs的小文件
      }.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
    }

    //用户表数据
    def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) {
      import sparkSession.implicits._
      ssc.textFile("/user/zhang/ods/member.log").filter(item => {
        val obj: JSONObject = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions {
        partition => {
          partition.map { item =>
            val jsonObect: JSONObject = ParseJsonData.getJsonData(item)
            val ad_id: String = jsonObect.getString("ad_id")
            val birthday: String = jsonObect.getString("birthday")
            val email: String = jsonObect.getString("email")
            val fullname: String = jsonObect.getString("fullname").substring(0, 1) + "xx"
            val iconurl: String = jsonObect.getString("iconurl")
            val lastlogin: String = jsonObect.getString("lastlogin")
            val mailaddr: String = jsonObect.getString("mailaddr")
            val memberlevel: String = jsonObect.getString("memberlevel")
            val password = "******"
            val paymoney: String = jsonObect.getString("paymoney")
            val phone: String = jsonObect.getString("phone")
            val newphone: String = phone.substring(0, 3) + "******" + phone.substring(7, 11)
            val qq: String = jsonObect.getString("qq")
            val register: String = jsonObect.getString("register")
            val regupdatetime: String = jsonObect.getString("regupdatetime")
            val uid: String = jsonObect.getString("uid")
            val unitname: String = jsonObect.getString("unitname")
            val userip: String = jsonObect.getString("userip")
            val zipcode: String = jsonObect.getString("zipcode")
            val dt: String = jsonObect.getString("dt")
            val dn: String = jsonObect.getString("dn")
            (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
              register, regupdatetime, unitname, userip, zipcode, dt, dn)
          }
        }
      }.toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
    }

    //导入广告表
    def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) {
      import sparkSession.implicits._
      ssc.textFile("/user/zhang/ods/baseadlog.log").filter(item => {
        val obj: JSONObject = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }
      ).mapPartitions {
        partition => {
          partition.map {
            item => {
              val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
              val adid: String = jSONObject.getString("adid")
              val adname: String = jSONObject.getString("adname")
              val dn: String = jSONObject.getString("dn")
              (adid, adname, dn)
            }
          }
        }
      }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
    }

    //导入网站表基础数据
    def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession){
      import sparkSession.implicits._
      ssc.textFile("/user/zhang/ods/baswewebsite.log").filter(item=>{
        val obj: JSONObject = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>{
        partition.map(item=>{
          val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
          val siteid: String = jSONObject.getString("siteid")
          val sitename: String = jSONObject.getString("sitename")
          val siteurl: String = jSONObject.getString("siteurl")
          val delete: String = jSONObject.getString("delete")
          val createtime: String = jSONObject.getString("createtime")
          val creator: String = jSONObject.getString("creator")
          val dn: String = jSONObject.getString("dn")
          (siteid, sitename, siteurl, delete, createtime, creator, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
    }
    //导入用户付款信息
    def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession){
      import sparkSession.implicits._
      ssc.textFile("/user/zhang/ods/pcentermempaymoney.log").filter(item=>{
        val obj: JSONObject = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>{
        partition.map(item=>{
          val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
          val paymoney: String = jSONObject.getString("paymoney")
          val uid: String = jSONObject.getString("uid")
          val vip_id: String = jSONObject.getString("vip_id")
          val site_id: String = jSONObject.getString("siteid")
          val dt: String = jSONObject.getString("dt")
          val dn: String = jSONObject.getString("dn")
          (uid, paymoney, site_id, vip_id, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
    }

    //导入用户vip基础数据
    def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession): Unit ={
      import sparkSession.implicits._
      ssc.textFile("/user/zhang/ods/pcenterMemViplevel.log").filter(item=>{
        val obj: JSONObject = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>{
        partition.map(item=>{
          val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
          val discountval: String = jSONObject.getString("discountval")
          val end_time: String = jSONObject.getString("end_time")
          val last_modify_time: String = jSONObject.getString("last_modify_time")
          val max_free: String = jSONObject.getString("max_free")
          val min_free: String = jSONObject.getString("min_free")
          val next_level: String = jSONObject.getString("next_level")
          val operator: String = jSONObject.getString("operator")
          val start_time: String = jSONObject.getString("start_time")
          val vip_id: String = jSONObject.getString("vip_id")
          val vip_level: String = jSONObject.getString("vip_level")
          val dn: String = jSONObject.getString("dn")
          (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
    }

}
