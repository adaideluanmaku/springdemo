package com.fourfire.guajie.dao;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.fourfire.guajie.service.ShardeDBBLL.DATATYPE;
import com.google.common.collect.Lists;
import com.medicom.modules.persistence.JdbcGenericMysqlDao;

/**
 * 
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  ShardeDBDao </li>
 * <li>类描述：表到分表   </li>
 * <li>创建人：jy </li>
 * <li>创建时间：2017年6月7日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Repository
public class ShardeDBDao extends JdbcGenericMysqlDao {
	private final Logger logger = LoggerFactory.getLogger(ShardeDBDao.class);

	
	/**
	 * <ul>
	 * <li>方法名：  hospPatientIndex </li>
	 * <li>功能描述：5.3  住院病人指标 </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月27日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	public void hospPatientIndex() {
		String sql = "";
		try {
			/** 5.1.2.1 tmp_indtmp_ clinicpres预处理过程 */

			// 清洗数据
			logger.info("5.3.1.2.1	tmp_indtmp_hosppt预处理数据->开始删除数据");
			sql = "truncate table tmp_indtmp_hosppt";
			getJdbcTemplate().execute(sql);

			// 5.3.1.2.1.1 antinum, anticost, antitype1, antitype2, antilevel1, antilevel2,
			// antilevel3预处理
			sql = "";
			sql += " insert into tmp_indtmp_hosppt(mhiscode,caseid,antinum,anticost,antitype1,antitype2,antilevel1,antilevel2,antilevel3) ";
			sql += " select a.mhiscode, a.caseid,COUNT(DISTINCT a.itemcode) AS antinum,sum(a.cost) AS anticost,  ";
			sql += " MAX(CASE c.antitype WHEN 1 THEN 1 ELSE NULL END) AS antitype1, ";
			sql += " MAX(CASE c.antitype WHEN 2 THEN 1 ELSE NULL END) AS antitype2, ";
			sql += " MAX(CASE c.antilevel WHEN 1 THEN 1 ELSE NULL END) AS antilevel1,  ";
			sql += " MAX(CASE c.antilevel WHEN 2 THEN 1 ELSE NULL END) AS antilevel2,  ";
			sql += " MAX(CASE c.antilevel WHEN 3 THEN 1 ELSE NULL END) AS antilevel3 ";
			sql += " FROM mc_inhosp_drugcostdistinct as a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c on b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode ";
			sql += " WHERE (c.is_anti = 1) AND (c.antitype IN (1, 2)) AND (a.is_extuse = 0) ";
			sql += " GROUP BY a.mhiscode,a.caseid ";
			getJdbcTemplate().execute(sql);

			// 5.3.1.2.1.2 unionnum预处理
			// 第一步
			sql = "";
			sql += " INSERT INTO tmp_indtmp_hosppt(mhiscode,caseid,unionnum) ";
			sql += " select  mhiscode, caseid, unionnum from ( ";
			sql += " SELECT   mhiscode, caseid, MAX(unionnum) AS unionnum FROM  ( ";
			sql += " SELECT   mhiscode, caseid, cid, COUNT(DISTINCT ordercode) AS unionnum FROM  ( ";
			sql += " SELECT   modm.mhiscode, modm.caseid, modm.cid, modm2.ordercode FROM      mc_inhosp_drugorder_main AS modm  ";
			sql += " INNER JOIN mc_inhosp_drugorder_main AS modm2 ON modm.caseid = modm2.caseid AND modm.mhiscode = modm2.mhiscode AND modm.startdatetime BETWEEN modm2.startdatetime AND modm2.pa_enddatetime ";
			sql += " WHERE   (modm.is_out = 0) AND (modm.is_use = 1) AND (modm2.is_out = 0) ";
			sql += " AND (modm2.is_use = 1) AND EXISTS ( ";
			sql += " SELECT   1 AS Expr1 FROM      mc_dict_drug AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme ";
			sql += " WHERE   (a.drugcode = modm.ordercode) AND (b.mhiscode = modm.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2)) ";
			sql += " ) AND  EXISTS  (";
			sql += " SELECT   1 AS Expr1 FROM      mc_dict_drug AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme ";
			sql += " WHERE   (a.drugcode = modm2.ordercode) AND (b.mhiscode = modm2.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2)) ";
			sql += " )  ";
			sql += " AND EXISTS (	 ";
			sql += " SELECT   1 AS Expr1 FROM   mc_dict_route AS c  ";
			sql += " INNER JOIN mc_hospital_match_relation AS d ON c.match_scheme = d.routematch_scheme ";
			sql += " WHERE   (c.routecode = modm.routecode) AND (d.mhiscode = modm.mhiscode) AND (c.route_type IN (1, 2, 5)) ";
			sql += " ) AND EXISTS( ";
			sql += " SELECT  1 AS Expr1 FROM      mc_dict_route AS c  ";
			sql += " INNER JOIN mc_hospital_match_relation AS d ON  c.match_scheme = d.routematch_scheme ";
			sql += " WHERE   (c.routecode = modm2.routecode) AND (d.mhiscode = modm2.mhiscode) AND (c.route_type IN (1, 2, 5)) ";
			sql += " ) ";
			sql += " ) AS x GROUP BY mhiscode, caseid, cid ";
			sql += " ) AS y GROUP BY mhiscode, caseid ";
			sql += " ) z ";
			sql += " where not exists( select mhiscode,caseid,unionnum from tmp_indtmp_hosppt where z.caseid = caseid and z.mhiscode = mhiscode ) ";
			getJdbcTemplate().execute(sql);

			// 第二步
			sql = "";
			sql += " update  tmp_indtmp_hosppt x,( ";
			sql += " SELECT  mhiscode, caseid, MAX(unionnum) AS unionnum FROM  ( ";
			sql += " SELECT   mhiscode, caseid, cid, COUNT(DISTINCT ordercode) AS unionnum FROM ( ";
			sql += " SELECT   modm.mhiscode, modm.caseid, modm.cid, modm2.ordercode FROM  mc_inhosp_drugorder_main AS modm  ";
			sql += " INNER JOIN mc_inhosp_drugorder_main AS modm2 ON modm.caseid = modm2.caseid AND modm.mhiscode = modm2.mhiscode AND modm.startdatetime BETWEEN modm2.startdatetime AND modm2.pa_enddatetime  ";
			sql += " WHERE   (modm.is_out = 0) AND (modm.is_use = 1) AND (modm2.is_out = 0) AND (modm2.is_use = 1) ";
			sql += " AND EXISTS ( ";
			sql += " SELECT 1 AS Expr1 FROM mc_dict_drug AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme ";
			sql += " WHERE   (a.drugcode = modm.ordercode) AND (b.mhiscode = modm.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2)) ";
			sql += " ) AND  EXISTS  ( ";
			sql += " SELECT   1 AS Expr1 FROM      mc_dict_drug AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme  ";
			sql += " WHERE   (a.drugcode = modm2.ordercode)  AND (b.mhiscode = modm2.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2)) ";
			sql += " )  AND EXISTS(	 ";
			sql += " SELECT   1 AS Expr1 FROM      mc_dict_route AS c  ";
			sql += " INNER JOIN mc_hospital_match_relation AS d ON c.match_scheme = d.routematch_scheme ";
			sql += " WHERE   (c.routecode = modm.routecode) AND (d.mhiscode = modm.mhiscode) AND (c.route_type IN (1, 2, 5)) ";
			sql += "  )  AND EXISTS ( ";
			sql += " SELECT   1 AS Expr1 FROM  mc_dict_route AS c  ";
			sql += " INNER JOIN mc_hospital_match_relation AS d ON  c.match_scheme = d.routematch_scheme   ";
			sql += " WHERE   (c.routecode = modm2.routecode) AND (d.mhiscode = modm2.mhiscode) AND (c.route_type IN (1, 2, 5)) ) ";
			sql += " ) AS x GROUP BY mhiscode, caseid, cid ";
			sql += " ) AS y GROUP BY mhiscode, caseid ";
			sql += " ) z set x.unionnum = z.unionnum  ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode ";
			getJdbcTemplate().execute(sql);

			// 5.3.1.2.1.3 is_labpt, is_labtype1预处理
			// 第一步：
			sql = "";
			sql += " INSERT INTO tmp_indtmp_hosppt(mhiscode, caseid, is_labpt, is_labtype1)  ";
			sql += " select  mhiscode, caseid, is_labpt, is_labtype1 from (  ";
			sql += " SELECT  moc.mhiscode, moc.caseid, MAX(CASE WHEN mdc.is_byx IN (2, 3) THEN 1 ELSE 0 END) AS is_labpt, 1 AS is_labtype1  ";
			sql += " FROM mc_inhosp_cost AS moc   ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = moc.mhiscode   ";
			sql += " INNER JOIN mc_dict_costitem AS mdc ON mhmr.costitemmatch_scheme = mdc.match_scheme AND mdc.itemcode = moc.itemcode AND mdc.itemtype = 3 AND mdc.is_byx > 0  ";
			sql += " WHERE (moc.costtype = 3) GROUP BY moc.mhiscode, moc.caseid  ";
			sql += " ) z  ";
			sql += " where not exists( select mhiscode,caseid,is_labpt,is_labtype1 from tmp_indtmp_hosppt where z.caseid = caseid and z.mhiscode = mhiscode)   ";
			getJdbcTemplate().execute(sql);

			// 第二步
			sql = "";
			sql += " update  tmp_indtmp_hosppt x,( ";
			sql += " SELECT   moc.mhiscode, moc.caseid, MAX(CASE WHEN mdc.is_byx IN (2, 3) THEN 1 ELSE 0 END) AS is_labpt, 1 AS is_labtype1 ";
			sql += " FROM mc_inhosp_cost AS moc  ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = moc.mhiscode  ";
			sql += " INNER JOIN mc_dict_costitem AS mdc ON mhmr.costitemmatch_scheme = mdc.match_scheme AND mdc.itemcode = moc.itemcode AND mdc.itemtype = 3 AND mdc.is_byx > 0 ";
			sql += " WHERE   (moc.costtype = 3) GROUP BY moc.mhiscode, moc.caseid ";
			sql += " ) z set x.is_labpt = z.is_labpt,x.is_labtype1 = z.is_labtype1  ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode  ";
			getJdbcTemplate().execute(sql);

			// 5.3.1.2.1.4 is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl预处理
			sql = "";
			sql += " INSERT INTO tmp_indtmp_hosppt(mhiscode, caseid, is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl) ";
			sql += " select mhiscode, caseid, is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl from ( ";
			sql += " SELECT   a.mhiscode, a.caseid,  ";
			sql += " MAX(CASE WHEN a.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND a.antilevel = 2 THEN 1 ELSE 0 END) AS is_xz_zl, ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND  a.antilevel = 3 THEN 1 ELSE 0 END) AS is_ts_zl,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND a.antilevel = 1 THEN 1 ELSE 0 END) AS is_fxz_zl ";
			sql += " FROM  (SELECT  a.mhiscode, a.caseid, a.cid, a.ordercode AS drugcode, a.routecode, a.deptcode,  ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway, a.antilevel FROM  ( ";
			sql += " SELECT   a.mhiscode, a.caseid, a.cid, a.ordercode, a.routecode, a.deptcode, a.doctorcode, a.is_out, a.is_allow, a.startdatetime, ";
			sql += " a.pa_enddatetime AS enddatetime, a.meditime, a.executetime, d.antitype, d.antilevel, e.route_type ";
			sql += " FROM       mc_inhosp_drugorder_main AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS c ON c.mhiscode = a.mhiscode  ";
			sql += " INNER JOIN mc_dict_drug AS d ON c.drugmatch_scheme = d.match_scheme AND a.ordercode = d.drugcode  ";
			sql += " INNER JOIN mc_dict_route AS e ON c.routematch_scheme = e.match_scheme AND a.routecode = e.routecode ";
			sql += " WHERE   (d.is_anti = 1) AND (d.antitype IN (1, 2)) AND (a.is_use = 1) AND (e.isskintest <> 1) ) a  ";
			sql += " INNER JOIN ";
			sql += " ( SELECT   a.caseid, a.mhiscode,  distype1, distype2 FROM      mc_inhosp_patient_baseinfo a  ";
			sql += " LEFT JOIN	( SELECT   x.mhiscode, x.caseid, (case when z.dis_type=1 then 1 else 0 end) AS distype1, (case when z.dis_type =2 then 2 else 0 end) as distype2 ";
			sql += " FROM      mc_inhosp_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON y.mhiscode = x.mhiscode  ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 1 or z.dis_type =2   GROUP BY x.mhiscode, x.caseid )  ";
			sql += " b ON a.mhiscode = b.mhiscode AND a.caseid = b.caseid   ) d ON a.caseid = d .caseid AND a.mhiscode = d .mhiscode  ";
			sql += " WHERE   NOT EXISTS	( SELECT   1 FROM      pre_inhosp_druguseway WHERE   mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid  ) )  AS a  ";
			sql += " INNER JOIN mc_dict_route AS d ON d.routecode = a.routecode ";
			sql += " WHERE   (d.route_type IN (1, 2, 5))   	GROUP BY a.mhiscode, a.caseid ";
			sql += " ) z where not exists( select mhiscode, caseid, is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl from tmp_indtmp_hosppt where z.caseid = caseid and z.mhiscode = mhiscode ) ";
			getJdbcTemplate().execute(sql);

			// 第二步
			sql = "";
			sql += " update  tmp_indtmp_hosppt x,( ";
			sql += " SELECT   a.mhiscode, a.caseid,  ";
			sql += " MAX(CASE WHEN a.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND a.antilevel = 2 THEN 1 ELSE 0 END) AS is_xz_zl, ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND  a.antilevel = 3 THEN 1 ELSE 0 END) AS is_ts_zl, ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND a.antilevel = 1 THEN 1 ELSE 0 END) AS is_fxz_zl ";
			sql += " FROM  (SELECT  a.mhiscode, a.caseid, a.cid, a.ordercode AS drugcode, a.routecode, a.deptcode,  ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway, a.antilevel ";
			sql += " FROM  ( SELECT   a.mhiscode, a.caseid, a.cid, a.ordercode, a.routecode, a.deptcode, a.doctorcode, a.is_out, a.is_allow, a.startdatetime,  ";
			sql += " a.pa_enddatetime AS enddatetime, a.meditime, a.executetime, d.antitype, d.antilevel, e.route_type ";
			sql += " FROM   mc_inhosp_drugorder_main AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS c ON c.mhiscode = a.mhiscode  ";
			sql += " INNER JOIN mc_dict_drug AS d ON c.drugmatch_scheme = d.match_scheme AND a.ordercode = d.drugcode  ";
			sql += " INNER JOIN mc_dict_route AS e ON c.routematch_scheme = e.match_scheme AND a.routecode = e.routecode ";
			sql += " WHERE   (d.is_anti = 1) AND (d.antitype IN (1, 2)) AND (a.is_use = 1) AND (e.isskintest <> 1) ";
			sql += " ) a  INNER JOIN ( ";
			sql += " SELECT   a.caseid, a.mhiscode,  distype1, distype2 FROM      mc_inhosp_patient_baseinfo a ";
			sql += " LEFT JOIN	( SELECT   x.mhiscode, x.caseid, (case when z.dis_type=1 then 1 else 0 end) AS distype1, (case when z.dis_type =2 then 2 else 0 end) as distype2 ";
			sql += " FROM      mc_inhosp_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON y.mhiscode = x.mhiscode  ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 1 or z.dis_type =2   GROUP BY x.mhiscode, x.caseid ";
			sql += " )  ";
			sql += " b ON a.mhiscode = b.mhiscode AND a.caseid = b.caseid   ";
			sql += " ) d ON a.caseid = d .caseid AND a.mhiscode = d .mhiscode  ";
			sql += " WHERE   NOT EXISTS	( SELECT   1 FROM      pre_inhosp_druguseway WHERE   mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid  ) ";
			sql += " )  AS a  ";
			sql += " INNER JOIN mc_dict_route AS d ON d.routecode = a.routecode ";
			sql += " WHERE   (d.route_type IN (1, 2, 5))   GROUP BY a.mhiscode, a.caseid ";
			sql += " ) z set x.is_yf = z.is_yf,x.is_zl = z.is_zl,x.is_xz_zl= z.is_xz_zl,x.is_ts_zl=z.is_ts_zl,x.is_fxz_zl = z.is_fxz_zl  ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode  ";
			getJdbcTemplate().execute(sql);

			// 5.3.1.2.1.5 is_anti_po,
			// is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt预处理
			sql = "";
			sql += " INSERT INTO tmp_indtmp_hosppt(mhiscode, caseid, is_anti_po, is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt) ";
			sql += "  select mhiscode, caseid, is_anti_po, is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt from  ";
			sql += "  ( ";
			sql += " SELECT   mhiscode, caseid,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 2 AND x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_anti_ivgtt,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_anti_formtype1,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_formtype1,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_ivgtt ";
			sql += " FROM   ( SELECT   a.mhiscode, a.caseid, c.is_anti, c.antitype, c.drugformtype, d.route_type, d.abbrev FROM      mc_inhosp_drugcost_caseid AS a  ";
			sql += " 	INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode  ";
			sql += " 	INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode  ";
			sql += " 	INNER JOIN mc_dict_route AS d ON d.match_scheme = b.routematch_scheme AND a.routecode = d.routecode ";
			sql += " 	WHERE   (a.is_use = 1) AND (c.drugformtype IN (1, 2)) ";
			sql += " ) AS x GROUP BY mhiscode, caseid ) z ";
			sql += " where not exists( ";
			sql += " select mhiscode, caseid, is_anti_po, is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt from tmp_indtmp_hosppt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode  ) ";
			getJdbcTemplate().execute(sql);

			// 第二步
			sql = "";
			sql += " update  tmp_indtmp_hosppt x,( SELECT   mhiscode, caseid,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 2 AND x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po, ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt, ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_anti_ivgtt,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_anti_formtype1,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_formtype1,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_ivgtt ";
			sql += " FROM ( SELECT   a.mhiscode, a.caseid, c.is_anti, c.antitype, c.drugformtype, d.route_type, d.abbrev ";
			sql += " FROM  mc_inhosp_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode   ";
			sql += " INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode  ";
			sql += " INNER JOIN mc_dict_route AS d ON d.match_scheme = b.routematch_scheme AND a.routecode = d.routecode ";
			sql += " WHERE   (a.is_use = 1) AND (c.drugformtype IN (1, 2)) ) AS x GROUP BY mhiscode, caseid ";
			sql += " ) z set x.is_anti_po = z.is_anti_po,x.is_anti_im = z.is_anti_im, x.is_anti_iv=z.is_anti_iv,x.is_anti_iv_ivgtt=z.is_anti_iv_ivgtt,x.is_anti_ivgtt=z.is_anti_ivgtt, ";
			sql += " x.is_anti_formtype1 = z.is_anti_formtype1,x.is_formtype1=z.is_formtype1,x.is_ivgtt = z.is_ivgtt ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode   ";
			getJdbcTemplate().execute(sql);

			// 5.3.1.2.2 tmp_indtmp_hospptcost预处理数据
			sql = "";
			sql += " INSERT into tmp_indtmp_hospptcost(mhiscode, caseid, cost, drugcost, base_cost, xy_cost, zcy_cost, zyyp_cost, swzj_cost) ";
			sql += " SELECT   x.mhiscode, x.caseid, x.cost, x.drugcost, y.base_cost, y.xy_cost, y.zcy_cost, y.zyyp_cost, y.swzj_cost ";
			sql += " FROM      mc_inhosp_patient_medinfo AS x  ";
			sql += " LEFT OUTER JOIN ( SELECT   a.mhiscode, a.caseid,  ";
			sql += " SUM(CASE c.is_basedrug WHEN 1 THEN a.cost ELSE NULL END) AS base_cost,  ";
			sql += " SUM(CASE c.drugtype WHEN 3 THEN a.cost WHEN 4 THEN a.cost ELSE NULL END) AS xy_cost, ";
			sql += " SUM(CASE c.drugtype WHEN 1 THEN a.cost ELSE NULL END) AS zcy_cost,  ";
			sql += " SUM(CASE c.drugtype WHEN 2 THEN a.cost ELSE NULL END) AS zyyp_cost,  ";
			sql += " SUM(CASE c.drugtype WHEN 4 THEN a.cost ELSE NULL END) AS swzj_cost ";
			sql += " FROM      mc_inhosp_drugcostdistinct AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode  ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode  GROUP BY a.mhiscode, a.caseid )  ";
			sql += " AS y ON x.mhiscode = y.mhiscode AND x.caseid = y.caseid  ";
			getJdbcTemplate().execute(sql);

			// 5.3.1.2.3 tmp_indtmp_hospptnum预处理数据
			sql = "";
			sql += " insert into tmp_indtmp_hospptnum(mhiscode,caseid,drugnum,basenum) ";
			sql += " SELECT   x.mhiscode, x.caseid, x.drugnum, y.basenum ";
			sql += " FROM ( SELECT   mhiscode, caseid, COUNT(DISTINCT itemcode) AS drugnum ";
			sql += " FROM     mc_inhosp_drugcostdistinct AS mods  GROUP BY mhiscode, caseid ) AS x  ";
			sql += " LEFT OUTER JOIN ( ";
			sql += " SELECT   mods.mhiscode, mods.caseid, COUNT(DISTINCT mods.itemcode) AS basenum ";
			sql += "  FROM      mc_inhosp_drugcostdistinct AS mods  ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mods.mhiscode  ";
			sql += "  LEFT OUTER JOIN mc_dict_drug AS mdd ON mhmr.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = mods.itemcode ";
			sql += "  WHERE   (mdd.is_basedrug = 1)  GROUP BY mods.mhiscode, mods.caseid )  ";
			sql += " AS y ON y.mhiscode = x.mhiscode AND y.caseid = x.caseid ";
			getJdbcTemplate().execute(sql);

			logger.info("5.3  住院病人指标->结束");
		} catch (Exception e) {
			logger.error("5.3  住院病人指标->执行错误：" + e.getMessage(), e);
		}
	}

	
	/**
	 * 
	 * <ul>
	 * <li>方法名：  deleteShardeDBByDelCol </li>
	 * <li>功能描述：删除月表数据 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年7月12日 </li>
	 * </ul> 
	 * @param fromTB
	 * @param shardCol
	 * @param mouth
	 * @param ienddate '20130717'...
	 * @return
	 */
	public int deleteShardeDBByDelCol(String fromTB, String shardCol, String mouth, int ienddate) {
		String intoTB = fromTB + "_" + mouth;
		// 201706015959...
		String sql = "delete from " + intoTB + " where " + shardCol + " = ? ";
		return jdbcTemplate.update(sql, new Object[] { ienddate });
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  copyShardeDB </li>
	 * <li>功能描述： 按照月份拷贝全数据(201706015959...)</li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月7日 </li>
	 * </ul> 
	 * @param intoTB
	 * @param fromTB
	 */
	public int copyShardeDB_YYYYMM(String fromTB, String shardCol, String mouth) {
		String intoTB = fromTB + "_" + mouth;
		// 201706015959...
		String sql = "replace into " + intoTB + " select * from " + fromTB + " where substring(" + shardCol + ",5,2)=? ";
		return jdbcTemplate.update(sql, new Object[] { mouth });
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  copyShardeDB </li>
	 * <li>功能描述： 按照月份拷贝全数据(2017-06-02 59:59:59...)</li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月7日 </li>
	 * </ul> 
	 * @param intoTB
	 * @param fromTB
	 */
	public int copyShardeDB_YYYY_MM(String fromTB, String shardCol, String mouth) {
		String intoTB = fromTB + "_" + mouth;
		// 2017-06-02 59:59:59...
		String sql = "replace into " + intoTB + " select * from " + fromTB + " where substring(" + shardCol + ",6,2)=? ";
		return jdbcTemplate.update(sql, new Object[] { mouth });
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  deleteFromTb </li>
	 * <li>功能描述：清空表 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月7日 </li>
	 * </ul> 
	 * @param tablename
	 */
	public void deleteFromTb(String tablename) {
		String sql = "delete from " + tablename;
		jdbcTemplate.execute(sql);
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  getTime </li>
	 * <li>功能描述：获取时间范围 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月7日 </li>
	 * </ul> 
	 * @param shardCol
	 * @return
	 */
	private List<Map<String, Object>> getTime(String shardCol, String fromTB, DATATYPE timetype) {
		String sql = null;
		switch (timetype) {
		case YYYY_MM:
			sql = "select DISTINCT substring(" + shardCol + ",6,2) m from " + fromTB;
			break;
		case YYYYMM:
			sql = "select DISTINCT substring(" + shardCol + ",5,2) m from " + fromTB;
			break;
		}
		List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
		return lst;
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  getienddate </li>
	 * <li>功能描述：针对ienddate处理 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年7月12日 </li>
	 * </ul> 
	 * @param shardCol
	 * @param fromTB
	 * @param timetype
	 * @return
	 */
	private List<Map<String, Object>> getienddate(String delCol, String fromTB, DATATYPE timetype) {
		String sql = null;
		switch (timetype) {
		case YYYY_MM:
			sql = "select DISTINCT substring(" + delCol + ",1,10) m from " + fromTB;
			break;
		case YYYYMM:
			sql = "select DISTINCT substring(" + delCol + ",1,8) m from " + fromTB;
			break;
		}
		List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
		return lst;
	}

	public String[] getDelCol(String delCol, String fromTB, DATATYPE timetype) {
		Set<String> ienddateArr = new HashSet<String>();
		List<Map<String, Object>> mList = getienddate(delCol, fromTB, timetype);
		for (Map<String, Object> map : mList) {
			String m = ObjectUtils.toString(map.get("m")).replace("-", "");
			if (m.length() > 0) {
				ienddateArr.add(m);
			}
		}
		return ienddateArr.toArray(new String[0]);
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  getMouth </li>
	 * <li>功能描述：数据库分隔的月份 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月7日 </li>
	 * </ul> 
	 * @param shardCol
	 * @param fromTB
	 * @return
	 */
	public List<String> getMouth(String shardCol, String fromTB, DATATYPE timetype) {
		List<String> arr = Lists.newArrayList();
		List<Map<String, Object>> mList = getTime(shardCol, fromTB, timetype);
		if (mList != null && mList.size() > 0) {
			// 月份
			for (int i = 0; i < mList.size(); i++) {
				Map<String, Object> m = mList.get(i);
				String mouth = ObjectUtils.toString(m.get("m"));
				if (StringUtils.isNotBlank(mouth)) {
					arr.add(mouth);
				}
			}
		} 
		return arr;
	}

	/**计算最大的值*/
	public int etlShardDB_init(final String tbname, final String identityKey) {
		//
		int maxid = 0;
		String maxMouth = null;
		for (int i = 1; i <= 12; i++) {
			String intoTB = tbname + "_" + String.format("%02d", i);
			String sql = "select max(" + identityKey + ")as maxid from " + intoTB;
			List<Map<String, Object>> lst = getJdbcTemplate().queryForList(sql);
			if (lst != null && lst.size() > 0) {
				int identity = Integer.parseInt(ObjectUtils.toString(lst.get(0).get("maxid"), "0"));
				if (identity > maxid) {
					maxid = identity;// 最大id
					maxMouth = intoTB;// 最大月份
				}
			}
		}
		if (maxid > 1) {
			String sql = "select * from " + maxMouth + " limit 1 ";
			List<Map<String, Object>> lst = getJdbcTemplate().queryForList(sql);
			if (lst != null && lst.size() > 0) {
				Map<String, Object> maxMap = lst.get(0);
				for (Map.Entry<String, Object> m : maxMap.entrySet()) {
					Object o = m.getValue();
					if (o == null) {
						m.setValue("0");
					} else if (o instanceof String) {
						m.setValue("-");
					} else if (o instanceof Double || o instanceof Float || o instanceof Number || o instanceof Long || o instanceof BigDecimal) {
						m.setValue(0);
					}
				}
				maxid += 1;
				maxMap.put(identityKey, maxid);
				maxid = insertKeepKey(tbname, identityKey, maxMap);
			}
		}
		return maxid;
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  insertKeepKey </li>
	 * <li>功能描述：并不删除主键的 插入 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年7月17日 </li>
	 * </ul> 
	 * @param tablename
	 * @param idn
	 * @param datamap
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public int insertKeepKey(String tablename, String idn, Map<String, Object> datamap) {
		StringBuffer sb = new StringBuffer("insert into ");
		StringBuilder sv = new StringBuilder(" values( ");
		List<Object> val = Lists.newArrayList();
		sb.append(tablename);
		sb.append(" (");
		Iterator iter = datamap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String fname = String.valueOf(entry.getKey());// 瀛楁鍚�
			sb.append(fname + ",");
			sv.append("? ,");
			val.add(entry.getValue());
		}
		sb.replace(sb.length() - 1, sb.length(), ")");
		sv.replace(sv.length() - 1, sv.length(), ")");
		return this.insertAndGetKey(sb.append(sv).toString(), idn, val.toArray());
	}

}
