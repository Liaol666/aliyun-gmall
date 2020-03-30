package member.service

import member.bean.{DwsMember, DwsMember_Result, MemberZipper, MemberZipperResult}
import member.dao.DwdMemberDao
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

object DwsMemberService {

  def importMemberUseApi(sparkSession: SparkSession, dt: String) = {
    import sparkSession.implicits._
    //隐式转换
    //s是插位符，字符串插值允许使用者将变量引用直接插入处理过的字面字符中
    val dwdMember: Dataset[Row] = DwdMemberDao.getDwdMember(sparkSession).where(s"dt='${dt}'")
    //主用户表
    val dwdMemberRegType: DataFrame = DwdMemberDao.getDwdMemberRegType(sparkSession)
    val dwdBaseAd: DataFrame = DwdMemberDao.getDwdBaseAd(sparkSession)
    val dwdBaseWebSite: DataFrame = DwdMemberDao.getDwdBaseWebSite(sparkSession)
    val dwdPcentermemPayMoney: DataFrame = DwdMemberDao.getDwdPcentermemPayMoney(sparkSession)
    val dwdVipLevel: DataFrame = DwdMemberDao.getDwdVipLevel(sparkSession)
    //Seq是两个表join的条件，它可以字段去重，到时候select的时候不会出现两个相同的字段。
    // 如果a表和b表join关联的字段不一致，可以使用dwdMember.join(dwdMemberRegType,dwdMember("uid") === dwdMemberRegType("id"))
    import org.apache.spark.sql.functions.broadcast
    val result: Dataset[DwsMember] = dwdMember.join(dwdMemberRegType, Seq("uid", "dn"), "left")
      .join(dwdBaseAd, Seq("ad_id", "dn"), "left_outer")
      .join(dwdBaseWebSite, Seq("siteid", "dn"), "left_outer")
      .join(dwdPcentermemPayMoney, Seq("uid", "dn"), "left_outer")
      .join(dwdVipLevel, Seq("vip_id", "dn"), "left_outer")
      //select有两个作用，第一个是排序，第二个是过滤掉不用的字段
      .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
      , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
      , "appregurl", "bdp_uuid", "reg_createtime", "isranreg", "regsource", "regsourcename", "adname"
      , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
      "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
      , "vip_operator", "dt", "dn").as[DwsMember]
    //将结果缓存，下次提取数据不用重新跑，设置缓存级别
    result.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val resultData: Dataset[DwsMember_Result] = result.groupByKey(item => {
      item.uid + "_" + item.dn
      //mapGroup算子，是把key和key的那组数据拿出来
    }).mapGroups { case (key, ite) => {
      val keys: Array[String] = key.split("_")
      val uid: Int = Integer.parseInt(keys(0))
      val dn: String = keys(1)
      //组内的数据是个迭代器，只能使用一次，如果需要反复使用，需要转成list
      val dwsMembers: List[DwsMember] = ite.toList
      //reduceOption为了防止有null出现，有过有null出现置为0.00
      val paymoney: String = dwsMembers.filter(_.paymoney != null).map(_.paymoney).reduceOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString()
      val ad_id: Int = dwsMembers.map(_.ad_id).head
      //head取第一个
      val fullname: String = dwsMembers.map(_.fullname).head
      val icounurl: String = dwsMembers.map(_.iconurl).head
      val lastlogin: String = dwsMembers.map(_.lastlogin).head
      val mailaddr: String = dwsMembers.map(_.mailaddr).head
      val memberlevel: String = dwsMembers.map(_.memberlevel).head
      val password: String = dwsMembers.map(_.password).head
      val phone: String = dwsMembers.map(_.phone).head
      val qq: String = dwsMembers.map(_.qq).head
      val register: String = dwsMembers.map(_.register).head
      val regupdatetime: String = dwsMembers.map(_.regupdatetime).head
      val unitname: String = dwsMembers.map(_.unitname).head
      val userip: String = dwsMembers.map(_.userip).head
      val zipcode: String = dwsMembers.map(_.zipcode).head
      val appkey: String = dwsMembers.map(_.appkey).head
      val appregurl: String = dwsMembers.map(_.appregurl).head
      val bdp_uuid: String = dwsMembers.map(_.bdp_uuid).head
      val reg_createtime: String = dwsMembers.map(_.reg_createtime).head
      val isranreg: String = dwsMembers.map(_.isranreg).head
      val regsource: String = dwsMembers.map(_.regsource).head
      val regsourcename: String = dwsMembers.map(_.regsourcename).head
      val adname: String = dwsMembers.map(_.adname).head
      val siteid: String = dwsMembers.map(_.siteid).head
      val sitename: String = dwsMembers.map(_.sitename).head
      val siteurl: String = dwsMembers.map(_.siteurl).head
      val site_delete: String = dwsMembers.map(_.site_delete).head
      val site_createtime: String = dwsMembers.map(_.site_createtime).head
      val site_creator: String = dwsMembers.map(_.site_creator).head
      val vip_id: String = dwsMembers.map(_.vip_id).head
      val vip_level: String = dwsMembers.map(_.vip_level).max
      val vip_start_time: String = dwsMembers.map(_.vip_start_time).min
      val vip_end_time: String = dwsMembers.map(_.vip_end_time).max
      val vip_last_modify_time: String = dwsMembers.map(_.vip_last_modify_time).max
      val vip_max_free: String = dwsMembers.map(_.vip_max_free).head
      val vip_min_free: String = dwsMembers.map(_.vip_min_free).head
      val vip_next_level: String = dwsMembers.map(_.vip_next_level).head
      val vip_operator: String = dwsMembers.map(_.vip_operator).head
      //超过了22个字段所以需要使用样例类
      DwsMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
        phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
        bdp_uuid, reg_createtime, isranreg, regsource, regsourcename, adname, siteid,
        sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
        vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
        vip_next_level, vip_operator, dt, dn)
    }
    }
    resultData.show()
//    while (true) {
//      println("1")
//    }
  }

  def importMember(sparkSession: SparkSession, time: String) = {
    import sparkSession.implicits._
    sparkSession.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
      "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
      "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
      "first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime)," +
      "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
      "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
      "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
      "first(vip_operator),dt,dn from" +
      "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
      "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
      "a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.isranreg,b.regsource," +
      "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
      "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
      "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
      "f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
      s"from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
      "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
      " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
      s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${time}')r  " +
      "group by uid,dn,dt").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")


    //查询当天增量数据
    val dayResult = sparkSession.sql(s"select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level," +
      s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,first(a.dn) as dn " +
      " from dwd.dwd_pcentermempaymoney a join " +
      s"dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn where a.dt='$time' group by uid").as[MemberZipper]

    //查询历史拉链表数据
    val historyResult = sparkSession.sql("select *from dws.dws_member_zipper").as[MemberZipper]

    //两份数据进行union，同时修改历史数据最后一条endtime改为当天新增start_time
    dayResult.union(historyResult).groupByKey(item => item.uid + "_" + item.dn)
      //对一组用户的数据进行操作
      .mapGroups { case (key, iters) => {
      val keys: Array[String] = key.split("_")
      val uid: String = keys(0)
      val dn: String = keys(1)
      //对开始时间进行排序
      val list: List[MemberZipper] = iters.toList.sortBy(iter => {
        iter.start_time
      })
      //如果list集合>1 历史拉链表的end时间=9999-12-31，那么就要修改历史的end时间
      if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
        //获取历史数据的最后一条
        val oldLastModel = list(list.size - 2)
        //获取当前时间的最后一条
        val lastModel = list(list.size - 1)
        oldLastModel.end_time = lastModel.start_time
        //最后的金额需要累加
        lastModel.paymoney = (BigDecimal.apply(lastModel.paymoney) + BigDecimal.apply(oldLastModel.paymoney)).toString()
      }
      //把这些list放到一个样例类中返回
      MemberZipperResult(list)
    }
      //因为封装到了样例类中，要把他们打散重新放到表里，使用flatmap
    }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")
  }
}
