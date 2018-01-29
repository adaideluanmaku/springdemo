package com.fourfire.dealother;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.medicom.modules.persistence.JdbcGenericMysqlDao;

/**
 * 
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  McClinicDao </li>
 * <li>类描述： 门诊冗余数据处理  </li>
 * <li>创建人：周科 </li>
 * <li>创建时间：2017年6月27日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Repository
public class McClinicDao extends JdbcGenericMysqlDao {

	private final Logger logger = LoggerFactory.getLogger(McClinicDao.class);

	/**
	 * 
	 * <ul>
	 * <li>方法名：  deal </li>
	 * <li>功能描述：总处理方法 </li>
	 * <li>创建人：  周科 </li>
	 * <li>创建时间：2017年6月27日 </li>
	 * </ul>
	 */
	public void deal(String startdate, String enddate) {
		try {
			logger.info("[门诊冗余数据处理]--开始");

			// 主键更新
			updatePrimaryKey();
			// 统计字段更新
			updateStatistics(startdate, enddate);

			logger.info("[门诊冗余数据处理]--结束");

		} catch (Exception ex) {
			log.error("门诊冗余数据处理", ex);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  updatePrimaryKey </li>
	 * <li>功能描述： 主键更新</li>
	 * <li>创建人：  zk </li>
	 * <li>创建时间：2017年6月27日 </li>
	 * </ul>
	 */
	private void updatePrimaryKey() {
		try {
			String sql = "UPDATE mc_clinic_prescription a  SET a.GOV_CPB_ID=(SELECT b.GOV_CPB_ID from mc_clinic_patient_medinfo b where  "
					+ "b.caseid = a.caseid	        AND b.ienddate = a.ienddate	        AND b.mhiscode = a.mhiscode)"
					+ " where a.GOV_CPB_ID is null;";
			getJdbcTemplate().update(sql);

			String sql1 = "UPDATE mc_clinic_drugorder_main a  SET a.GOV_CPRSP_ID=(SELECT b.GOV_CPRSP_ID from mc_clinic_prescription b where  "
					+ "b.caseid = a.caseid	        AND b.ienddate = a.ienddate	        AND b.mhiscode = a.mhiscode  AND b.prescno = a.prescno )"
					+ " where a.GOV_CPRSP_ID is null;";
			getJdbcTemplate().update(sql1);

			// 在院
			String sqlinhosp = "UPDATE mc_inhosp_drugorder_main a  SET a.GOV_CPRSP_ID=(SELECT b.GOV_CPRSP_ID from mc_inhosp_patient_medinfo b where  "
					+ " b.caseid = a.caseid	                AND b.mhiscode = a.mhiscode ) where a.GOV_CPRSP_ID is null";
			getJdbcTemplate().update(sqlinhosp);
			// 出院
			String sqlouthosp = "UPDATE mc_outhosp_drugorder_main a  SET a.GOV_CPRSP_ID=(SELECT b.GOV_CPRSP_ID from mc_outhosp_patient_medinfo b where  "
					+ " b. caseid = a.caseid	   AND b.ienddate = a.ienddate	        AND b.mhiscode = a.mhiscode) where a.GOV_CPRSP_ID is null";
			getJdbcTemplate().update(sqlouthosp);

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  updateStatistics </li>
	 * <li>功能描述： 更新统计字段</li>
	 * <li>创建人：  zk </li>
	 * <li>创建时间：2017年6月27日 </li>
	 * </ul>
	 */
	private void updateStatistics(String startdate, String enddate) {
		try {
			// 药品品种数
			String sql = "UPDATE mc_clinic_prescription a  RIGHT  JOIN (   "
					+ "select md.GOV_CPRSP_ID, mod1.ienddate,mod1.mhiscode,mod1.caseid,mod1.prescno, count(distinct mod1.itemcode) AS drugnum   "
					+ "from mc_clinic_prescription md  INNER JOIN  mc_clinic_drugcost_caseid mod1 on md.caseid=mod1.caseid and  md.mhiscode=mod1.mhiscode  and  md.prescno=mod1.prescno  "
					+ "INNER JOIN  mc_hospital_match_relation mhmr on mhmr.mhiscode = mod1.mhiscode "
					+ "left join  mc_dict_drug mdd on  mod1.itemcode = mdd.drugcode   and  mhmr.drugmatch_scheme = mdd.match_scheme   "
					+ "left join mc_dict_route mdr on mdr.routecode = mod1.routecode  and  mhmr.routematch_scheme = mdr.match_scheme  "
					+ "where  ( ifnull(mdd.is_solvent,-1)  <> 1  or ifnull(mdr.route_type,-1) not in (2,3))    "
					+ "group by mod1.ienddate,mod1.mhiscode,mod1.caseid,mod1.prescno    "
					+ ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID   set a.drugnum=b.drugnum "
					+ " where a.ienddate>=? and a.ienddate<?";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			// 基本药物(种)
			String sql1 = "UPDATE mc_clinic_prescription a  RIGHT JOIN (  "
					+ "select md.GOV_CPRSP_ID, mod1.ienddate AS ienddate,mod1.mhiscode AS mhiscode,mod1.caseid AS caseid,mod1.prescno AS prescno, count(distinct mod1.itemcode) AS basenum  "
					+ "from mc_clinic_prescription md  INNER JOIN  mc_clinic_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode and md.prescno=mod1.prescno  "
					+ "INNER JOIN  mc_hospital_match_relation mhmr on mhmr.mhiscode = mod1.mhiscode    "
					+ "left join  mc_dict_drug mdd on  mhmr.drugmatch_scheme = mdd.match_scheme  and  mdd.drugcode = mod1.itemcode  and  mdd.is_basedrug = 1  "
					+ "group by mod1.ienddate,mod1.mhiscode,mod1.caseid,mod1.prescno  "
					+ ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID   set a.basenum=b.basenum"
					+ " where a.ienddate>=? and a.ienddate<?";
			getJdbcTemplate().update(sql1, new Object[] { startdate, enddate });
			// 中成药
			String sql2 = "UPDATE mc_clinic_prescription a  RIGHT JOIN (   "
					+ "select md.GOV_CPRSP_ID, mod1.ienddate,mod1.mhiscode,mod1.caseid,mod1.prescno,count(distinct mod1.itemcode) AS zcy_num    "
					+ "from mc_clinic_prescription md  INNER JOIN  mc_clinic_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode and md.prescno=mod1.prescno "
					+ "INNER JOIN  mc_hospital_match_relation mhmr on mhmr.mhiscode = mod1.mhiscode   "
					+ "left join  mc_dict_drug mdd on  mhmr.drugmatch_scheme = mdd.match_scheme  and  mdd.drugcode = mod1.itemcode  where  mdd.drugtype = 1  "
					+ "group by mod1.ienddate,mod1.mhiscode,mod1.caseid,mod1.prescno "
					+ ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID set a.zcy_num=b.zcy_num "
					+ "where a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql2, new Object[] { startdate, enddate });
			// 注射剂
			String sql3 = "UPDATE mc_clinic_prescription a RIGHT JOIN( "
					+ " SELECT md.GOV_CPRSP_ID, mod1.ienddate, mod1.mhiscode, mod1.caseid, mod1.prescno, 1 AS is_formtype1  "
					+ " FROM mc_clinic_prescription md  "
					+ " INNER JOIN mc_clinic_drugcost_caseid mod1  ON mod1.caseid = md.caseid and mod1.ienddate= md.ienddate  and mod1.prescno= md.prescno  and mod1.mhiscode= md.mhiscode   "
					+ " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = mod1.mhiscode  "
					+ " INNER JOIN mc_dict_drug mdd ON mhmr.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = mod1.itemcode  AND mdd.drugformtype = 1  "
					+ " LEFT JOIN mc_dict_route mdr ON mdr.routecode = mod1.routecode AND mhmr.routematch_scheme = mdr.match_scheme AND mdr.route_type <> 2  "
					+ " WHERE  mod1.is_use=1 and mdr.routecode is null  " + " GROUP BY mod1.ienddate, mod1.mhiscode, mod1.caseid, mod1.prescno  "
					+ " )b ON a.GOV_CPRSP_ID = b.GOV_CPRSP_ID  SET a.is_formtype1 = b.is_formtype1"
					+ " where a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql3, new Object[] { startdate, enddate });
			// 抗菌药 特殊抗菌药
			String sql4 = "UPDATE mc_clinic_prescription a  RIGHT JOIN (   "
					+ " select X.GOV_CPRSP_ID,X.ienddate AS ienddate,X.mhiscode AS mhiscode,X.caseid AS caseid,X.prescno AS prescno,count(distinct X.ordercode) AS antinum,max((case X.antilevel when 3 then 1 else NULL end)) AS is_antilevel3 "
					+ " from (select md.GOV_CPRSP_ID,mod1.ienddate AS ienddate,mod1.mhiscode AS mhiscode,mod1.caseid AS caseid,mod1.prescno AS prescno,mod1.itemcode AS ordercode,c.antilevel AS antilevel  "
					+ " from mc_clinic_prescription md  INNER JOIN  mc_clinic_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode and  md.prescno=mod1.prescno "
					+ " INNER join mc_hospital_match_relation b on mod1.mhiscode = b.mhiscode   "
					+ " INNER join mc_dict_drug c on  b.drugmatch_scheme = c.match_scheme  and  c.drugcode = mod1.itemcode   and  c.is_anti = 1  and c.antitype in (1,2)   "
					+ " INNER join mc_dict_route d on b.routematch_scheme = d.match_scheme  and mod1.routecode = d.routecode  and  d.route_type in (1,2,5)   "
					+ " ) X  group by X.ienddate,X.mhiscode,X.caseid,X.prescno   "
					+ " ) b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID  set a.antinum=b.antinum ,a.is_antilevel3=b.is_antilevel3"
					+ " where a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql4, new Object[] { startdate, enddate });
			// 抗肿瘤药数量 及 金额
			String sql5 = "UPDATE mc_clinic_prescription a  RIGHT  JOIN ( " + "SELECT     mod1.ienddate, mod1.caseid, mod1.prescno, mod1.mhiscode, "
					+ "           COUNT(DISTINCT mod1.itemcode) AS cancer_num, SUM(mod1.cost) AS cancer_cost  "
					+ "FROM       mc_clinic_drugcost_caseid AS mod1 INNER JOIN "
					+ "           mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mod1.mhiscode INNER JOIN "
					+ "           mc_dict_drug AS mdr ON mod1.itemcode = mdr.drugcode AND mhmr.drugmatch_scheme = mdr.match_scheme INNER JOIN "
					+ "           mc_dict_dnc AS mdn ON mdr.classid = mdn.classid " + "WHERE     (mod1.is_use = 1) AND (mdn.fullpath LIKE '肿瘤用药%') "
					+ "GROUP BY mod1.ienddate, mod1.caseid, mod1.prescno, mod1.mhiscode) b  "
					+ "on a.ienddate=b.ienddate  and a.caseid=b.caseid and a.prescno=b.prescno and a.mhiscode=b.mhiscode "
					+ "set a.cancer_num=b.cancer_num,a.cancer_cost=b.cancer_cost "
					+ " where a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql5, new Object[] { startdate, enddate });
			// 抗菌药费用
			String sql6 = "UPDATE mc_clinic_prescription a  RIGHT  JOIN ( " + "SELECT     mod1.ienddate, mod1.caseid, mod1.prescno, mod1.mhiscode, "
					+ "           SUM(mod1.cost) AS anti_cost  " + "FROM       mc_clinic_drugcost_caseid AS mod1 INNER JOIN "
					+ "           mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mod1.mhiscode INNER JOIN "
					+ "           mc_dict_drug AS mdr ON mod1.itemcode = mdr.drugcode AND mhmr.drugmatch_scheme = mdr.match_scheme INNER JOIN "
					+ "           mc_dict_dnc AS mdn ON mdr.classid = mdn.classid " + "WHERE     (mod1.is_use = 1) AND (mdn.fullpath LIKE '抗感染药%') "
					+ "GROUP BY mod1.ienddate, mod1.caseid, mod1.prescno, mod1.mhiscode) b  "
					+ "on a.ienddate=b.ienddate  and a.caseid=b.caseid and a.prescno=b.prescno and a.mhiscode=b.mhiscode "
					+ "set a.anti_cost=b.anti_cost "
					+ " where a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql6, new Object[] { startdate, enddate });

			// 出院
			// 药品品种数
			String outsql = "UPDATE mc_outhosp_patient_medinfo a  RIGHT  JOIN (    "
					+ "select md.GOV_CPRSP_ID, mod1.ienddate,mod1.mhiscode,mod1.caseid, count(distinct mod1.itemcode) AS drugnum  "
					+ "from mc_outhosp_patient_medinfo md  INNER JOIN  mc_outhosp_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode  "
					+ "group by mod1.ienddate,mod1.mhiscode,mod1.caseid    "// 
					+ ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID  set a.drugnum=b.drugnum"
					+ " where a.ienddate>=? and a.ienddate<?";
			getJdbcTemplate().update(outsql, new Object[] { startdate, enddate });
			// 基本药物(种)
			String outsql1 = "UPDATE mc_outhosp_patient_medinfo a  RIGHT JOIN (  "
					+ "select md.GOV_CPRSP_ID, mod1.ienddate AS ienddate,mod1.mhiscode AS mhiscode,mod1.caseid AS caseid, count(distinct mod1.itemcode) AS basenum  "
					+ "from mc_outhosp_patient_medinfo md  INNER JOIN  mc_outhosp_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode "
					+ "INNER JOIN  mc_hospital_match_relation mhmr on mhmr.mhiscode = mod1.mhiscode    "
					+ "left join  mc_dict_drug mdd on  mhmr.drugmatch_scheme = mdd.match_scheme  and  mdd.drugcode = mod1.itemcode  and  mdd.is_basedrug = 1   "
					+ "group by mod1.ienddate,mod1.mhiscode,mod1.caseid  "//
					+ ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID  set a.basenum=b.basenum "
					+ " where a.ienddate>=? and a.ienddate<?";
			getJdbcTemplate().update(outsql1, new Object[] { startdate, enddate });

			// 抗菌药 特殊抗菌药
			String outsql4 = "UPDATE mc_outhosp_patient_medinfo a  RIGHT JOIN (   "
					+ "select X.GOV_CPRSP_ID,X.ienddate AS ienddate,X.mhiscode AS mhiscode,X.caseid AS caseid,count(distinct X.ordercode) AS antinum,max((case X.antilevel when 3 then 1 else NULL end)) AS is_antilevel3  "
					+ "from (select md.GOV_CPRSP_ID,mod1.ienddate AS ienddate,mod1.mhiscode AS mhiscode,mod1.caseid AS caseid,mod1.itemcode AS ordercode,c.antilevel AS antilevel   "
					+ "from mc_outhosp_patient_medinfo md  INNER JOIN  mc_outhosp_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode   "
					+ "INNER join mc_hospital_match_relation b on mod1.mhiscode = b.mhiscode   "
					+ "INNER join mc_dict_drug c on  b.drugmatch_scheme = c.match_scheme  and  c.drugcode = mod1.itemcode   and  c.is_anti = 1  and c.antitype in (1,2)    "
					+ "INNER join mc_dict_route d on b.routematch_scheme = d.match_scheme  and mod1.routecode = d.routecode  and  d.route_type in (1,2,5)    "
					+ ") X  group by X.ienddate,X.mhiscode,X.caseid   "
					+ ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID  set a.antinum=b.antinum ,a.is_antilevel3=b.is_antilevel3"
					+ " where a.ienddate>=? and a.ienddate<?";
			getJdbcTemplate().update(outsql4, new Object[] { startdate, enddate });
			// 抗肿瘤药数量 及 金额
			String outsql5 = "UPDATE mc_outhosp_patient_medinfo a  RIGHT  JOIN ( " //
					+ "SELECT     mod1.ienddate, mod1.caseid, mod1.mhiscode, "
					+ "           COUNT(DISTINCT mod1.itemcode) AS cancer_num, SUM(mod1.cost) AS cancer_cost  "
					+ "FROM       mc_outhosp_drugcost_caseid AS mod1 INNER JOIN "
					+ "           mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mod1.mhiscode INNER JOIN "
					+ "           mc_dict_drug AS mdr ON mod1.itemcode = mdr.drugcode AND mhmr.drugmatch_scheme = mdr.match_scheme INNER JOIN "
					+ "           mc_dict_dnc AS mdn ON mdr.classid = mdn.classid "//
					+ "WHERE     (mod1.is_use = 1) AND (mdn.fullpath LIKE '肿瘤用药%') "
					+ "GROUP BY mod1.ienddate, mod1.caseid, mod1.mhiscode) b  "
					+ "on a.ienddate=b.ienddate  and a.caseid=b.caseid   and a.mhiscode=b.mhiscode "
					+ "set a.cancer_num=b.cancer_num,a.cancer_cost=b.cancer_cost "
					+" where a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(outsql5, new Object[] { startdate, enddate });
			// 抗菌药费用
			String outsql6 = "UPDATE mc_outhosp_patient_medinfo a  RIGHT  JOIN ( " //
			+ "SELECT     mod1.ienddate, mod1.caseid,mod1.mhiscode, "
					+ "           SUM(mod1.cost) AS anti_cost  " + "FROM       mc_outhosp_drugcost_caseid AS mod1 INNER JOIN "
					+ "           mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mod1.mhiscode INNER JOIN "
					+ "           mc_dict_drug AS mdr ON mod1.itemcode = mdr.drugcode AND mhmr.drugmatch_scheme = mdr.match_scheme INNER JOIN "
					+ "           mc_dict_dnc AS mdn ON mdr.classid = mdn.classid " 
					+ "WHERE     (mod1.is_use = 1) AND (mdn.fullpath LIKE '抗感染药%') "//
					+ "GROUP BY mod1.ienddate, mod1.caseid, mod1.mhiscode) b  "//
					+ "on a.ienddate=b.ienddate  and a.caseid=b.caseid and a.mhiscode=b.mhiscode " //
					+ "set a.anti_cost=b.anti_cost "
					+" where a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(outsql6, new Object[] { startdate, enddate });

			// 在院

			// 药品品种数
			String insql = "UPDATE mc_inhosp_patient_medinfo a  RIGHT  JOIN (  "
					+ "select md.GOV_CPRSP_ID, mod1.mhiscode,mod1.caseid, count(distinct mod1.itemcode) AS drugnum    "
					+ "from mc_inhosp_patient_medinfo md  INNER JOIN  mc_inhosp_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode "
					+ "group by mod1.mhiscode,mod1.caseid    " + ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID  set a.drugnum=b.drugnum";
			getJdbcTemplate().update(insql);
			// 基本药物(种)
			String insql1 = "UPDATE mc_inhosp_patient_medinfo a  RIGHT JOIN (   "
					+ "select md.GOV_CPRSP_ID, mod1.mhiscode AS mhiscode,mod1.caseid AS caseid, count(distinct mod1.itemcode) AS basenum  "
					+ "from mc_inhosp_patient_medinfo md  INNER JOIN  mc_inhosp_drugcost_caseid mod1 on md.caseid=mod1.caseid and  md.mhiscode=mod1.mhiscode "
					+ "INNER JOIN  mc_hospital_match_relation mhmr on mhmr.mhiscode = mod1.mhiscode   "
					+ "left join  mc_dict_drug mdd on  mhmr.drugmatch_scheme = mdd.match_scheme  and  mdd.drugcode = mod1.itemcode  and  mdd.is_basedrug = 1  "
					+ "group by mod1.mhiscode,mod1.caseid  " + ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID  set a.basenum=b.basenum";
			getJdbcTemplate().update(insql1);

			// 抗菌药 特殊抗菌药
			String insql4 = " UPDATE mc_inhosp_patient_medinfo a  RIGHT JOIN (  "
					+ "select X.GOV_CPRSP_ID,X.mhiscode AS mhiscode,X.caseid AS caseid,count(distinct X.ordercode) AS antinum,max((case X.antilevel when 3 then 1 else NULL end)) AS is_antilevel3  "
					+ "from (select md.GOV_CPRSP_ID,mod1.mhiscode AS mhiscode,mod1.caseid AS caseid,mod1.itemcode AS ordercode,c.antilevel AS antilevel   "
					+ "from mc_inhosp_patient_medinfo md  INNER JOIN  mc_inhosp_drugcost_caseid mod1 on md.caseid=mod1.caseid and md.mhiscode=mod1.mhiscode  "
					+ "INNER join mc_hospital_match_relation b on mod1.mhiscode = b.mhiscode   "
					+ "INNER join mc_dict_drug c on  b.drugmatch_scheme = c.match_scheme  and  c.drugcode = mod1.itemcode  and  c.is_anti = 1  and c.antitype in (1,2)    "
					+ "INNER join mc_dict_route d on b.routematch_scheme = d.match_scheme  and mod1.routecode = d.routecode  and  d.route_type in (1,2,5)    "
					+ ") X  group by X.mhiscode,X.caseid   " + ") b on a.GOV_CPRSP_ID=b.GOV_CPRSP_ID    "
					+ "set a.antinum=b.antinum ,a.is_antilevel3=b.is_antilevel3  ";
			getJdbcTemplate().update(insql4);

			// 抗肿瘤药数量 及 金额
			String insql5 = "UPDATE mc_outhosp_patient_medinfo a  RIGHT  JOIN ( " + "SELECT     mod1.caseid, mod1.mhiscode, "
					+ "           COUNT(DISTINCT mod1.itemcode) AS cancer_num, SUM(mod1.cost) AS cancer_cost  "
					+ "FROM       mc_outhosp_drugcost_caseid AS mod1 INNER JOIN "
					+ "           mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mod1.mhiscode INNER JOIN "
					+ "           mc_dict_drug AS mdr ON mod1.itemcode = mdr.drugcode AND mhmr.drugmatch_scheme = mdr.match_scheme INNER JOIN "
					+ "           mc_dict_dnc AS mdn ON mdr.classid = mdn.classid " + "WHERE     (mod1.is_use = 1) AND (mdn.fullpath LIKE '肿瘤用药%') "
					+ "GROUP BY mod1.caseid, mod1.mhiscode) b  " + "on a.caseid=b.caseid   and a.mhiscode=b.mhiscode "
					+ "set a.cancer_num=b.cancer_num,a.cancer_cost=b.cancer_cost; ";
			getJdbcTemplate().update(insql5);
			// 抗菌药费用
			String insql6 = "UPDATE mc_outhosp_patient_medinfo a  RIGHT  JOIN ( " + "SELECT     mod1.caseid,mod1.mhiscode, "
					+ "           SUM(mod1.cost) AS anti_cost  " + "FROM       mc_outhosp_drugcost_caseid AS mod1 INNER JOIN "
					+ "           mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mod1.mhiscode INNER JOIN "
					+ "           mc_dict_drug AS mdr ON mod1.itemcode = mdr.drugcode AND mhmr.drugmatch_scheme = mdr.match_scheme INNER JOIN "
					+ "           mc_dict_dnc AS mdn ON mdr.classid = mdn.classid " + "WHERE     (mod1.is_use = 1) AND (mdn.fullpath LIKE '抗感染药%') "
					+ "GROUP BY  mod1.caseid, mod1.mhiscode) b  " + "on  a.caseid=b.caseid and a.mhiscode=b.mhiscode "
					+ "set a.anti_cost=b.anti_cost; ";
			getJdbcTemplate().update(insql6);

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

	}

}
