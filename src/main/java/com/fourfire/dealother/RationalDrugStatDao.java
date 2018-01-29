package com.fourfire.dealother;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.medicom.modules.persistence.JdbcGenericMysqlDao;

/**
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  RationalDrugStatDao </li>
 * <li>类描述： 合理用药指标统计数据预处理Dao  </li>
 * <li>创建人：maYJ </li>
 * <li>创建时间：2017年7月10日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Repository
public class RationalDrugStatDao extends JdbcGenericMysqlDao {

	private final Logger logger = LoggerFactory.getLogger(RationalDrugStatDao.class);

	/**药品处理的主方法*/
	public void deal(String startdate, String enddate) {
		try {
			// 换算时间格式
			int startdateInt = Integer.parseInt(startdate.replace("-", ""));
			int enddateInt = Integer.parseInt(enddate.replace("-", ""));

			// 5.1 门诊处方指标
			clinicPrescriptionIndex(startdateInt, enddateInt);

			// 5.2 门诊病人指标
			clinicPatientIndex(startdateInt, enddateInt);

			// 5.3 住院病人指标
			hospPatientIndex(startdateInt, enddateInt);

			// 5.4 抗菌药物使用量
			kjy_use_num(startdateInt, enddateInt);

			// 5.5 抗菌药物使用强度
			indtmpSyqd(startdateInt, enddateInt);
		} catch (Exception ex) {
			logger.error("5.X 药品处理的主方法异常", ex);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  clinicPrescriptionIndex </li>
	 * <li>功能描述：5.1  门诊处方指标 </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月10日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void clinicPrescriptionIndex(int startdate, int enddate) {
		String sql = "";
		try {
			/**  5.1.2.1	tmp_indtmp_ clinicpres预处理过程	*/

			// 清洗数据
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始删除数据，startdate=" + startdate + "，enddate=" + enddate);
			for (int i = 1; i <= 12; i++) {
				// 根据日期删除数据
				sql = String.format("delete from tmp_indtmp_clinicpres_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

				sql = String.format("delete from tmp_indtmp_cost_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

				sql = String.format("delete from tmp_indtmp_num_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			}
			
			sql = "delete from tmp_indtmp_clinicpres where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			sql = "delete from tmp_indtmp_cost where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			sql = "delete from tmp_indtmp_num where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 1. 5.1.2.1.1 处理字段antinum, anticost, is_antitype1, is_antitype2, is_antilevel1,
			// is_antilevel2, is_antilevel3, unionnum
			logger.info(
					"合理用药指标统计数据预处理->5.1  门诊处方指标->开始1.  5.1.2.1.1	处理字段antinum, anticost, is_antitype1, is_antitype2, is_antilevel1, is_antilevel2, is_antilevel3, unionnum");
			sql = "";
			sql += " insert into tmp_indtmp_clinicpres (ienddate,mhiscode,caseid,prescno,antinum,anticost,is_antitype1,is_antitype2,is_antilevel1,is_antilevel2,is_antilevel3,unionnum) ";
			sql += " select x.ienddate,x.mhiscode,x.caseid,x.prescno,COUNT(DISTINCT x.itemcode) as antinum ,SUM(x.cost)  as anticost,  ";
			sql += " MAX(CASE x.antitype WHEN 1 THEN 1 ELSE NULL END) as is_antitype1 , ";
			sql += " MAX(CASE x.antitype WHEN 2 THEN 1 ELSE NULL END) as is_antitype2 , ";
			sql += " MAX(CASE x.antilevel WHEN 1 THEN 1 ELSE NULL END) as is_antilevel1, ";
			sql += " MAX(CASE x.antilevel WHEN 2 THEN 1 ELSE NULL END) as is_antilevel2, ";
			sql += " MAX(CASE x.antilevel WHEN 3 THEN 1 ELSE NULL END) as is_antilevel3, ";
			sql += " COUNT(DISTINCT x.itemcode) AS unionnum ";
			sql += " FROM  ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, a.itemcode, a.cost, c.antitype, c.antilevel ";
			sql += " FROM  mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type IN (1, 2, 5) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";
			sql += " ) AS x  ";
			sql += " GROUP BY ienddate, mhiscode, caseid, prescno ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 2. 5.1.2.1.2 处理字段is_ivdrip,is_antiivdrip,is_antiinjection
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始2.  5.1.2.1.2	处理字段is_ivdrip,is_antiivdrip,is_antiinjection");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpres(ienddate,mhiscode,caseid,prescno,is_ivdrip,is_antiivdrip,is_antiinjection) ";
			sql += " select ienddate,mhiscode,caseid,prescno,is_ivdrip,is_antiivdrip,is_antiinjection ";
			sql += " from ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, MAX(CASE WHEN c.abbrev = 'ivgtt' THEN 1 ELSE 0 END) AS is_ivdrip, ";
			sql += " MAX(CASE WHEN c.abbrev = 'ivgtt' AND d .is_anti = 1 AND d .antitype IN (1, 2) THEN 1 ELSE 0 END) AS is_antiivdrip, ";
			sql += " MAX(CASE WHEN c.route_type = 2 AND d .is_anti = 1 AND d .antitype IN (1, 2) AND d .drugformtype = 1 THEN 1 ELSE 0 END) AS is_antiinjection ";
			sql += " FROM  mc_clinic_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode  ";
			sql += " INNER JOIN mc_dict_route AS c ON b.routematch_scheme = c.match_scheme AND a.routecode = c.routecode  ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS d ON b.drugmatch_scheme = d.match_scheme AND a.itemcode = d.drugcode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<?  ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid, a.prescno) x ";
			sql += " WHERE not exists (select ienddate,mhiscode,caseid,caseid from tmp_indtmp_clinicpres ";
			sql += " where x.caseid = caseid and x.mhiscode = mhiscode and x.prescno = prescno and x.ienddate = ienddate and x.ienddate>=? and x.ienddate<?  ";
			sql += " ) ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 3. 5.1.2.1.2 更新字段is_ivdrip,is_antiivdrip,is_antiinjection
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始3.  5.1.2.1.2	更新字段is_ivdrip,is_antiivdrip,is_antiinjection");
			sql = "";
			sql += " update  tmp_indtmp_clinicpres x,( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno,MAX(CASE WHEN c.abbrev = 'ivgtt' THEN 1 ELSE 0 END) AS is_ivdrip,  ";
			sql += " MAX(CASE WHEN c.abbrev = 'ivgtt' AND d .is_anti = 1 AND d .antitype IN (1, 2) THEN 1 ELSE 0 END) AS is_antiivdrip, ";
			sql += " MAX(CASE WHEN c.route_type = 2 AND d .is_anti = 1 AND d .antitype IN (1, 2) AND d .drugformtype = 1 THEN 1 ELSE 0 END) AS is_antiinjection ";
			sql += " FROM 	mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_dict_route AS c ON b.routematch_scheme = c.match_scheme AND a.routecode = c.routecode ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS d ON b.drugmatch_scheme = d.match_scheme AND a.itemcode = d.drugcode ";
			sql += " WHERE  a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid, a.prescno ";
			sql += " ) z ";
			sql += " set x.is_ivdrip = z.is_ivdrip ,x.is_antiivdrip = z.is_antiivdrip,x.is_antiinjection = z.is_antiinjection ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.prescno = z.prescno and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 4. 5.1.2.1.3 预处理字段is_formtype1
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始4.  5.1.2.1.3	预处理字段is_formtype1");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpres (ienddate,mhiscode,caseid,prescno,is_formtype1)  ";
			sql += " select ienddate,mhiscode,caseid,prescno,is_formtype1 ";
			sql += " from (  ";
			sql += " SELECT   ienddate, mhiscode, caseid, prescno, 1 AS is_formtype1 ";
			sql += " FROM ( ";
			sql += " SELECT DISTINCT a.ienddate, a.mhiscode, a.caseid, a.prescno ";
			sql += " FROM  mc_clinic_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND a.itemcode = c.drugcode AND c.drugformtype = 1 ";
			sql += " LEFT OUTER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type<> 2 ";
			sql += " WHERE   (a.is_use = 1) AND (d.routecode IS NULL) and a.ienddate>=? and a.ienddate<? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid, a.prescno) AS s ";
			sql += " GROUP BY ienddate, mhiscode, caseid, prescno ";
			sql += " ) x ";
			sql += " WHERE x.mhiscode = 9  and  not exists (select ienddate,mhiscode,caseid,caseid from tmp_indtmp_clinicpres ";
			sql += " where x.caseid = caseid and x.mhiscode = mhiscode and x.prescno = prescno and x.ienddate = ienddate and x.ienddate>=? and x.ienddate<?) ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 5. 1.2.1.3 更新字段is_formtype1
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始5.  1.2.1.3	更新字段is_formtype1");
			sql = "";
			sql += " update  tmp_indtmp_clinicpres x, ";
			sql += " (SELECT   ienddate, mhiscode, caseid, prescno, 1 AS is_formtype1 ";
			sql += " FROM (  ";
			sql += " SELECT DISTINCT a.ienddate, a.mhiscode, a.caseid, a.prescno ";
			sql += " FROM  mc_clinic_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode  ";
			sql += " INNER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND a.itemcode = c.drugcode AND c.drugformtype = 1 ";
			sql += " LEFT OUTER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type<> 2 ";
			sql += " WHERE   (a.is_use = 1) AND (d.routecode IS NULL) and a.ienddate>=? and a.ienddate<? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid, a.prescno ";
			sql += " ) AS s ";
			sql += " Where s.ienddate>=? and s.ienddate<? ";
			sql += " GROUP BY ienddate, mhiscode, caseid, prescno ";
			sql += " ) z ";
			sql += " set x.is_formtype1 = z.is_formtype1 ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.prescno = z.prescno and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate<? ; ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate, startdate, enddate });

			// 6. 5.1.2.1.4 预处理is_yf, is_zl
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始6.  5.1.2.1.4	预处理is_yf, is_zl");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpres(ienddate,mhiscode,caseid,prescno,is_yf,is_zl) ";
			sql += " select ienddate,mhiscode,caseid,prescno,is_yf,is_zl ";
			sql += " from ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, MAX(CASE WHEN c.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf, MAX(CASE WHEN c.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl ";
			sql += " FROM			 mc_clinic_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, useway ";
			sql += " FROM	    mc_clinic_drugorder_main a  ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN pre_clinic_druguseway d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno AND d .cid = a.cid ";
			sql += " where a.ienddate>=? and a.ienddate<?  ";
			sql += " UNION ALL ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway ";
			sql += " FROM   	mc_clinic_drugorder_main a  ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, IFNULL(b.distype, 0) AS distype1, IFNULL(c.distype, 0) AS distype2 ";
			sql += " FROM  mc_clinic_prescription a  ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 1 AS distype ";
			sql += " FROM      mc_clinic_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode  ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 1 and x.ienddate>=? and x.ienddate<? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) b ON b.ienddate = a.ienddate AND  b.mhiscode = a.mhiscode AND b.caseid = a.caseid AND b.prescno = a.prescno ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 2 AS distype ";
			sql += " FROM      mc_clinic_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 2 and x.ienddate>=? and x.ienddate<? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno ";
			sql += " where a.ienddate>=? and a.ienddate<?  ";
			sql += " ) d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno ";
			sql += " WHERE a.ienddate>=? and a.ienddate<?  ";
			sql += " and NOT EXISTS (SELECT 1 FROM pre_clinic_druguseway WHERE ienddate = a.ienddate AND mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid)  ";
			sql += " AND NOT EXISTS (SELECT 1 FROM mc_dict_route x, mc_hospital_match_relation y WHERE x.match_scheme = y.routematch_scheme AND y.mhiscode = a.mhiscode AND x.routecode = a.routecode AND isskintest = 1 ) ";
			sql += " ) AS c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno AND c.drugcode = a.itemcode ";
			sql += " INNER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type IN (1,2, 5) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<?  ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid, a.prescno ";
			sql += " ) x ";
			sql += " WHERE x.ienddate>=? and x.ienddate<? ";
			sql += " and not exists (select ienddate,mhiscode,caseid,prescno from tmp_indtmp_clinicpres where x.caseid = caseid and x.mhiscode = mhiscode and x.prescno = prescno and x.ienddate = ienddate and x.ienddate>=? and x.ienddate<?) ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate, startdate, enddate, startdate, enddate, startdate,
					enddate, startdate, enddate, startdate, enddate, startdate, enddate });

			// 7. 5.1.2.1.4 更新is_yf, is_zl
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始7.  5.1.2.1.4	更新is_yf, is_zl");
			sql = "";
			sql += " update  tmp_indtmp_clinicpres x, ";
			sql += " ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, MAX(CASE WHEN c.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf, MAX(CASE WHEN c.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl ";
			sql += " FROM			 mc_clinic_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, useway ";
			sql += " FROM	    mc_clinic_drugorder_main a  ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN pre_clinic_druguseway d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno AND d .cid = a.cid ";
			sql += " where a.ienddate>=? and a.ienddate<?  ";
			sql += " UNION ALL ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway ";
			sql += " FROM   	mc_clinic_drugorder_main a  ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.prescno, IFNULL(b.distype, 0) AS distype1, IFNULL(c.distype, 0) AS distype2 ";
			sql += " FROM  mc_clinic_prescription a  ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 1 AS distype ";
			sql += " FROM      mc_clinic_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode  ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 1 and x.ienddate>=? and x.ienddate<? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) b ON b.ienddate = a.ienddate AND  b.mhiscode = a.mhiscode AND b.caseid = a.caseid AND b.prescno = a.prescno ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 2 AS distype ";
			sql += " FROM      mc_clinic_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 2 and x.ienddate>=? and x.ienddate<? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno ";
			sql += " where a.ienddate>=? and a.ienddate<?  ";
			sql += " ) d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno ";
			sql += " WHERE a.ienddate>=? and a.ienddate<?  ";
			sql += " and NOT EXISTS (SELECT 1 FROM pre_clinic_druguseway WHERE ienddate = a.ienddate AND mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid)  ";
			sql += " AND NOT EXISTS (SELECT 1 FROM mc_dict_route x, mc_hospital_match_relation y WHERE x.match_scheme = y.routematch_scheme AND y.mhiscode = a.mhiscode AND x.routecode = a.routecode AND isskintest = 1 ) ";
			sql += " ) AS c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno AND c.drugcode = a.itemcode ";
			sql += " INNER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type IN (1,2, 5) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<?  ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid, a.prescno ";
			sql += " ) z ";
			sql += " set x.is_yf = z.is_yf ,x.is_zl = z.is_zl  ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.prescno = z.prescno and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate, startdate, enddate, startdate, enddate, startdate,
					enddate, startdate, enddate, startdate, enddate });

			// 8. 5.1.2.1.5 预处理is_anti_po, is_anti_im, is_anti_iv, is_anti_iv_ivgtt
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始8.  5.1.2.1.5	预处理is_anti_po, is_anti_im, is_anti_iv, is_anti_iv_ivgtt");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpres (ienddate, mhiscode, caseid, prescno,is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt) ";
			sql += " select ienddate, mhiscode, caseid, prescno,is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt ";
			sql += " from (  ";
			sql += " SELECT   ienddate, mhiscode, caseid, prescno, MAX(CASE WHEN drugformtype = 2 AND x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt ";
			sql += " FROM ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, c.drugformtype, d.route_type, d.abbrev ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode ";
			sql += " WHERE   (a.is_use = 1) and a.ienddate>=? and a.ienddate<? ";
			sql += " ) AS x GROUP BY ienddate, mhiscode, caseid, prescno ";
			sql += " ) z ";
			sql += " WHERE z.ienddate>=? and z.ienddate<? and not exists ( ";
			sql += " select ienddate,mhiscode,caseid,caseid from tmp_indtmp_clinicpres where z.caseid = caseid and z.mhiscode = mhiscode and z.prescno = prescno and z.ienddate = ienddate and z.ienddate>=? and z.ienddate<? ) ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate, startdate, enddate });

			// 9. 5.1.2.1.5 更新is_anti_po, is_anti_im, is_anti_iv, is_anti_iv_ivgtt
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始9.  5.1.2.1.5	更新is_anti_po, is_anti_im, is_anti_iv, is_anti_iv_ivgtt");
			sql = "";
			sql += " update  tmp_indtmp_clinicpres x, ";
			sql += " (  ";
			sql += " SELECT   ienddate, mhiscode, caseid, prescno, MAX(CASE WHEN drugformtype = 2 AND x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt ";
			sql += " FROM ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, c.drugformtype, d.route_type, d.abbrev ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode ";
			sql += " WHERE   (a.is_use = 1) and a.ienddate>=? and a.ienddate<? ";
			sql += " ) AS x GROUP BY ienddate, mhiscode, caseid, prescno ";
			sql += " ) z ";
			sql += " set x.is_anti_po = z.is_anti_po ,x.is_anti_im = z.is_anti_im,x.is_anti_iv = z.is_anti_iv,x.is_anti_iv_ivgtt=x.is_anti_iv_ivgtt ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.prescno = z.prescno and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			/**  10.  5.1.2.2 tmp_indtmp_clinicpres 预处理规则	*/
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始10.  5.1.2.2 tmp_indtmp_clinicpres 预处理规则");
			sql = "";
			sql += " insert into tmp_indtmp_cost( ienddate,mhiscode,caseid,prescno,cost,drugcost,base_cost,zcy_cost,is_zcy,is_zyyp ) ";
			sql += " SELECT  x.ienddate, x.mhiscode, x.caseid, x.prescno, x.cost, x.drugcost, y.base_cost, y.zcy_cost, y.is_zcy, y.is_zyyp ";
			sql += " FROM    mc_clinic_prescription AS x  ";
			sql += " LEFT OUTER JOIN ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, ";
			sql += " SUM(CASE c.is_basedrug WHEN 1 THEN a.cost ELSE NULL END) AS base_cost, ";
			sql += " SUM(CASE c.drugtype WHEN 1 THEN a.cost ELSE NULL END) AS zcy_cost, ";
			sql += " MAX(CASE WHEN c.drugtype = 1 AND a.is_use = 1 THEN 1 ELSE 0 END) AS is_zcy, ";
			sql += " MAX(CASE WHEN c.drugtype = 2 AND a.is_use = 1 THEN 1 ELSE 0 END) AS is_zyyp ";
			sql += " FROM      mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode ";
			sql += " Where a.ienddate>=? and a.ienddate<? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid, a.prescno ";
			sql += " ) AS y ON y.ienddate = x.ienddate AND y.mhiscode = x.mhiscode AND y.caseid = x.caseid AND y.prescno = x.prescno ";
			sql += " where x.ienddate>=? and x.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			/**  11.  5.1.2.3 tmp_indtmp_num预处理视图	*/
			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->开始11.  5.1.2.3 tmp_indtmp_num预处理视图");
			sql = "";
			sql += " insert  into tmp_indtmp_num ( ienddate,mhiscode,caseid,prescno,drugnum,basenum,zcy_num ) ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, y.drugnum, z.basenum, h.zcy_num ";
			sql += " FROM     mc_clinic_prescription AS x  ";
			sql += " LEFT OUTER JOIN  ( ";
			sql += " SELECT   mods.ienddate, mods.mhiscode, mods.caseid, mods.prescno, COUNT(DISTINCT mods.itemcode) AS drugnum ";
			sql += " FROM      mc_clinic_drugcost_caseid AS mods ";
			sql += " LEFT JOIN mc_dict_drug AS mdd ON mods.itemcode = mdd.drugcode AND mdd.match_scheme =0 ";
			sql += " LEFT JOIN mc_dict_route AS mdr ON mdr.routecode = mods.routecode AND mdr.match_scheme = 0 ";
			sql += " WHERE   (mods.is_use = 1) AND (mdd.is_solvent <> 1) OR (mods.is_use = 1) AND (mdr.route_type NOT IN (2, 3)) and mods.ienddate>=? and mods.ienddate<? ";
			sql += " GROUP BY mods.ienddate, mods.mhiscode, mods.caseid, mods.prescno ";
			sql += " ) AS y ";
			sql += " ON y.ienddate = x.ienddate AND y.mhiscode = x.mhiscode AND y.caseid = x.caseid AND y.prescno = x.prescno and y.ienddate>=? and y.ienddate<? ";
			sql += " LEFT OUTER JOIN ( ";
			sql += " SELECT   mods.ienddate, mods.mhiscode, mods.caseid, mods.prescno, COUNT(DISTINCT mods.itemcode) AS basenum ";
			sql += " FROM       mc_clinic_drugcost_caseid AS mods ";
			sql += " INNER JOIN mc_dict_drug AS mdd ON mdd.match_scheme=0  AND mdd.drugcode = mods.itemcode AND mdd.is_basedrug = 1 ";
			sql += " WHERE   (mods.is_use = 1)  and mods.ienddate>=? and mods.ienddate<? ";
			sql += " GROUP BY mods.ienddate, mods.mhiscode, mods.caseid, mods.prescno ";
			sql += " ) AS z  ";
			sql += " ON z.ienddate = x.ienddate AND z.mhiscode = x.mhiscode AND z.caseid = x.caseid AND z.prescno = x.prescno and z.ienddate>=? and z.ienddate<? ";
			sql += " LEFT OUTER JOIN( ";
			sql += " SELECT   mods.ienddate, mods.mhiscode, mods.caseid, mods.prescno, COUNT(DISTINCT mods.itemcode) AS zcy_num ";
			sql += " FROM       mc_clinic_drugcost_caseid AS mods ";
			sql += " INNER JOIN mc_dict_drug AS mdd ON mdd.match_scheme=0 AND mdd.drugcode = mods.itemcode AND mdd.drugtype = 1  ";
			sql += " WHERE   (mods.is_use = 1) and mods.ienddate>=? and mods.ienddate<? ";
			sql += " GROUP BY mods.ienddate, mods.mhiscode, mods.caseid, mods.prescno ";
			sql += " ) AS h  ";
			sql += " ON h.ienddate = x.ienddate AND h.mhiscode = x.mhiscode AND h.caseid = x.caseid AND h.prescno = x.prescno and h.ienddate>=? and h.ienddate<? ";
			sql += " where x.ienddate>=? and x.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate, startdate, enddate, startdate, enddate, startdate,
					enddate, startdate, enddate, startdate, enddate });

			logger.info("合理用药指标统计数据预处理->5.1  门诊处方指标->结束");
		} catch (Exception e) {
			logger.error("合理用药指标统计数据预处理->5.1  门诊处方指标->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  clinicPatientIndex </li>
	 * <li>功能描述：5.2  门诊病人指标</li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月12日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void clinicPatientIndex(int startdate, int enddate) {
		String sql = "";
		try {

			// 清洗数据
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始删除数据，startdate=" + startdate + "，enddate=" + enddate);
			for (int i = 1; i <= 12; i++) {
				// 根据日期删除数据
				sql = String.format("delete from tmp_indtmp_clinicpt_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

				sql = String.format("delete from tmp_indtmp_costpt_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

				sql = String.format("delete from tmp_indtmp_numpt_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			}
			
			sql = "delete from tmp_indtmp_clinicpt where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			sql = "delete from tmp_indtmp_costpt where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			sql = "delete from tmp_indtmp_numpt where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			/**  5.2.2.1	tmp_indtmp_clinicpt预处理过程	*/
			// 1. 5.2.2.1.1
			// antinum，anticost，is_antitype1，is_antitype2，is_antilevel1，is_antilevel2，is_antilevel3字段处理
			logger.info(
					"合理用药指标统计数据预处理->5.2  门诊病人指标->开始1.  5.2.2.1.1	antinum，anticost，is_antitype1，is_antitype2，is_antilevel1，is_antilevel2，is_antilevel3字段处理");
			sql = "";
			sql += " insert into tmp_indtmp_clinicpt(ienddate,mhiscode,caseid,antinum,anticost,is_antitype1,is_antitype2,is_antilevel1,is_antilevel2,is_antilevel3) ";
			sql += " SELECT   ienddate, mhiscode, caseid, COUNT(DISTINCT itemcode) AS antinum, SUM(cost) AS anticost, ";
			sql += " MAX(CASE antitype WHEN 1 THEN 1 ELSE NULL END) AS is_antitype1, ";
			sql += " MAX(CASE antitype WHEN 2 THEN 1 ELSE NULL  END) AS is_antitype2, ";
			sql += " MAX(CASE antilevel WHEN 1 THEN 1 ELSE NULL END) AS is_antilevel1, ";
			sql += " MAX(CASE antilevel WHEN 2 THEN 1 ELSE NULL END) AS is_antilevel2, ";
			sql += " MAX(CASE antilevel WHEN 3 THEN 1 ELSE NULL END) AS is_antilevel3 ";
			sql += " FROM  (SELECT  a.ienddate, a.mhiscode, a.caseid, a.itemcode, a.cost, c.antitype, c.antilevel ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN mc_dict_route AS d ON d.match_scheme = b.routematch_scheme AND a.routecode = d.routecode AND d.route_type IN (1, 2, 5) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate< ? ";
			sql += " ) AS x where x.ienddate>=? and x.ienddate<?  ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 2. 5.2.2.1.2 unionnum字段处理
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始2.  5.2.2.1.2	unionnum字段处理");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpt(ienddate,mhiscode,caseid,unionnum) ";
			sql += " select ienddate, mhiscode, caseid, unionnum from ( ";
			sql += " SELECT   ienddate, mhiscode, caseid, MAX(unionnum) AS unionnum ";
			sql += " FROM (  ";
			sql += " SELECT   ienddate, mhiscode, caseid, COUNT(DISTINCT itemcode) AS unionnum ";
			sql += " FROM (  ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.itemcode, LEFT(a.costtime, 10) AS day ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_dict_route AS c ON b.routematch_scheme = c.match_scheme AND a.routecode = c.routecode AND c.route_type IN (1, 2, 5) ";
			sql += " INNER JOIN mc_dict_drug AS d ON b.drugmatch_scheme = d.match_scheme AND a.itemcode = d.drugcode AND d.is_anti = 1 AND d.antitype IN (1, 2) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate< ? ";
			sql += " ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid, day ";
			sql += " ) AS y ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z ";
			sql += " where not exists( ";
			sql += " select ienddate,mhiscode,caseid,unionnum from tmp_indtmp_clinicpt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode  and z.ienddate = ienddate and ienddate>=? and ienddate< ? ";
			sql += " ) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 3. 5.2.2.1.2 unionnum字段更新
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始3.  5.2.2.1.2	unionnum字段更新");
			sql = "";
			sql += " update  tmp_indtmp_clinicpt x, ";
			sql += " ( ";
			sql += " SELECT   ienddate, mhiscode, caseid, MAX(unionnum) AS unionnum ";
			sql += " FROM (  ";
			sql += " SELECT   ienddate, mhiscode, caseid, COUNT(DISTINCT itemcode) AS unionnum ";
			sql += " FROM (  ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, a.itemcode, LEFT(a.costtime, 10) AS day ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_dict_route AS c ON b.routematch_scheme = c.match_scheme AND a.routecode = c.routecode AND c.route_type IN (1, 2, 5) ";
			sql += " INNER JOIN mc_dict_drug AS d ON b.drugmatch_scheme = d.match_scheme AND a.itemcode = d.drugcode AND d.is_anti = 1 AND d.antitype IN (1, 2) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate< ? ";
			sql += " ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid, day ";
			sql += " ) AS y ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z ";
			sql += " set x.unionnum = z.unionnum ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode  and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate< ? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 4. 5.2.2.1.3 is_formtype1字段处理
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始4.  5.2.2.1.3	is_formtype1字段处理");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpt(ienddate,mhiscode,caseid,is_formtype1) ";
			sql += " select ienddate, mhiscode, caseid, is_formtype1 ";
			sql += " from (  ";
			sql += " SELECT   ienddate, mhiscode, caseid, 1 AS is_formtype1 ";
			sql += " FROM ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON a.itemcode = c.drugcode AND b.drugmatch_scheme = c.match_scheme AND c.drugformtype = 1 ";
			sql += " LEFT OUTER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type <> 2 ";
			sql += " WHERE   (a.is_use = 1) AND (d.routecode IS NULL) and a.ienddate>=? and a.ienddate< ? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z ";
			sql += " where not exists( ";
			sql += " select ienddate,mhiscode,caseid,is_formtype1 from tmp_indtmp_clinicpt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode  and z.ienddate = ienddate and ienddate>=? and ienddate< ? ";
			sql += " ) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 5. 5.2.2.1.3 is_formtype1字段更新
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始5.  5.2.2.1.3	is_formtype1字段更新");
			sql = "";
			sql += " update  tmp_indtmp_clinicpt x, ";
			sql += " (  ";
			sql += " SELECT   ienddate, mhiscode, caseid, 1 AS is_formtype1 ";
			sql += " FROM ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON a.itemcode = c.drugcode AND b.drugmatch_scheme = c.match_scheme AND c.drugformtype = 1 ";
			sql += " LEFT OUTER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type <> 2 ";
			sql += " WHERE   (a.is_use = 1) AND (d.routecode IS NULL) and a.ienddate>=? and a.ienddate< ? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z ";
			sql += " set x.is_formtype1 = z.is_formtype1 ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode  and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate< ? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 6. 5.2.2.1.4 is_ivdrip，is_antiivdrip，is_antiinjection字段处理
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始6.  5.2.2.1.4	is_ivdrip，is_antiivdrip，is_antiinjection字段处理");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpt(ienddate, mhiscode, caseid, is_ivdrip,is_antiivdrip,is_antiinjection) ";
			sql += " select ienddate, mhiscode, caseid, is_ivdrip,is_antiivdrip,is_antiinjection ";
			sql += " from ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, MAX(CASE WHEN c.abbrev = 'ivgtt' THEN 1 ELSE 0 END) AS is_ivdrip, ";
			sql += " MAX(CASE WHEN c.abbrev = 'ivgtt' AND d .is_anti = 1 AND d .antitype IN (1, 2) THEN 1 ELSE 0 END) AS is_antiivdrip, ";
			sql += " MAX(CASE WHEN c.route_type = 2 AND d .is_anti = 1 AND d .antitype IN (1, 2) AND d .drugformtype = 1 THEN 1 ELSE 0 END) AS is_antiinjection ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_route AS c ON c.match_scheme = b.routematch_scheme  AND a.routecode = c.routecode ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS d ON d.match_scheme = b.drugmatch_scheme AND a.itemcode = d.drugcode ";
			sql += " WHERE a.is_use = 1 and a.mhiscode = 0 and a.ienddate>=? and a.ienddate< ? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) z ";
			sql += " where not exists( ";
			sql += " select ienddate,mhiscode,caseid, is_ivdrip,is_antiivdrip,is_antiinjection from tmp_indtmp_clinicpt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode  and z.ienddate = ienddate and ienddate>=? and ienddate< ? ";
			sql += " ) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 7. 5.2.2.1.4 is_ivdrip，is_antiivdrip，is_antiinjection字段更新
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始7.  5.2.2.1.4	is_ivdrip，is_antiivdrip，is_antiinjection字段更新");
			sql = "";
			sql += " update  tmp_indtmp_clinicpt x, ";
			sql += " ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, MAX(CASE WHEN c.abbrev = 'ivgtt' THEN 1 ELSE 0 END) AS is_ivdrip, ";
			sql += " MAX(CASE WHEN c.abbrev = 'ivgtt' AND d .is_anti = 1 AND d .antitype IN (1, 2) THEN 1 ELSE 0 END) AS is_antiivdrip, ";
			sql += " MAX(CASE WHEN c.route_type = 2 AND d .is_anti = 1 AND d .antitype IN (1, 2) AND d .drugformtype = 1 THEN 1 ELSE 0 END) AS is_antiinjection ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_route AS c ON c.match_scheme = b.routematch_scheme  AND a.routecode = c.routecode ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS d ON d.match_scheme = b.drugmatch_scheme AND a.itemcode = d.drugcode ";
			sql += " WHERE a.is_use = 1 and a.mhiscode = 0 and a.ienddate>=? and a.ienddate< ? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) z ";
			sql += " set x.is_ivdrip = z.is_ivdrip ,x.is_antiivdrip = z.is_antiivdrip,x.is_antiinjection = z.is_antiinjection ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode  and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate< ? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 8. 5.2.2.1.5 is_yf，is_zl字段处理
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始8.  5.2.2.1.5	is_yf，is_zl字段处理");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpt(ienddate,mhiscode,caseid,is_yf,is_zl) ";
			sql += " select ienddate, mhiscode, caseid, is_yf,is_zl ";
			sql += " from ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, MAX(CASE WHEN c.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf, ";
			sql += " MAX(CASE WHEN c.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, useway ";
			sql += " FROM       mc_clinic_drugorder_main a  ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN pre_clinic_druguseway d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno AND d .cid = a.cid ";
			sql += " where a.ienddate>=? and a.ienddate< ? ";
			sql += " UNION ALL ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway ";
			sql += " FROM       mc_clinic_drugorder_main a ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, IFNULL(b.distype, 0) AS distype1, IFNULL(c.distype, 0) AS distype2 ";
			sql += " FROM      mc_clinic_prescription a ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 1 AS distype ";
			sql += " FROM       mc_clinic_disease x ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 1 and x.ienddate>=? and x.ienddate< ? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) b ON b.ienddate = a.ienddate AND  b.mhiscode = a.mhiscode AND b.caseid = a.caseid AND b.prescno = a.prescno ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 2 AS distype ";
			sql += " FROM      mc_clinic_disease x ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 2 and x.ienddate>=? and x.ienddate< ? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno ";
			sql += " where a.ienddate>=? and a.ienddate< ? ";
			sql += " ) d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno ";
			sql += " WHERE  a.ienddate>=? and a.ienddate< ? ";
			sql += " and NOT EXISTS (SELECT 1 FROM pre_clinic_druguseway WHERE   ienddate = a.ienddate AND mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid) ";
			sql += " AND NOT EXISTS (SELECT 1 FROM mc_dict_route x, mc_hospital_match_relation y WHERE x.match_scheme = y.routematch_scheme AND y.mhiscode = a.mhiscode AND x.routecode = a.routecode AND isskintest = 1 ) ";
			sql += " ) AS c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno AND c.drugcode = a.itemcode ";
			sql += " INNER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type IN (1, 2, 5) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate< ? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) z ";
			sql += " where not exists( ";
			sql += " select ienddate,mhiscode,caseid,is_formtype1 from tmp_indtmp_clinicpt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode  and z.ienddate = ienddate and ienddate>=? and ienddate< ? ";
			sql += " ) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate, startdate, enddate, startdate, enddate, startdate,
					enddate, startdate, enddate, startdate, enddate });

			// 9. 5.2.2.1.5 is_yf，is_zl字段更新
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始9.  5.2.2.1.5	is_yf，is_zl字段更新");
			sql = "";
			sql += " update  tmp_indtmp_clinicpt x, ";
			sql += " ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, MAX(CASE WHEN c.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf, ";
			sql += " MAX(CASE WHEN c.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode ";
			sql += " INNER JOIN ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, useway ";
			sql += " FROM       mc_clinic_drugorder_main a  ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN pre_clinic_druguseway d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno AND d .cid = a.cid ";
			sql += " where a.ienddate>= ? and a.ienddate< ? ";
			sql += " UNION ALL ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, a.cid, a.ordercode AS drugcode, a.routecode, ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway ";
			sql += " FROM       mc_clinic_drugorder_main a ";
			sql += " INNER JOIN mc_hospital_match_relation b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.ordercode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.prescno, IFNULL(b.distype, 0) AS distype1, IFNULL(c.distype, 0) AS distype2 ";
			sql += " FROM      mc_clinic_prescription a ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 1 AS distype ";
			sql += " FROM       mc_clinic_disease x ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 1 and x.ienddate>=? and x.ienddate< ? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) b ON b.ienddate = a.ienddate AND  b.mhiscode = a.mhiscode AND b.caseid = a.caseid AND b.prescno = a.prescno ";
			sql += " LEFT JOIN( ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.prescno, 2 AS distype ";
			sql += " FROM      mc_clinic_disease x ";
			sql += " INNER JOIN mc_hospital_match_relation y ON x.mhiscode = y.mhiscode ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 2 and x.ienddate>=? and x.ienddate< ? ";
			sql += " GROUP BY x.ienddate, x.mhiscode, x.caseid, x.prescno ";
			sql += " ) c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno ";
			sql += " where a.ienddate>=? and a.ienddate< ? ";
			sql += " ) d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .prescno = a.prescno ";
			sql += " WHERE  a.ienddate>=? and a.ienddate< ? ";
			sql += " and NOT EXISTS (SELECT 1 FROM pre_clinic_druguseway WHERE   ienddate = a.ienddate AND mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid) ";
			sql += " AND NOT EXISTS (SELECT 1 FROM mc_dict_route x, mc_hospital_match_relation y WHERE x.match_scheme = y.routematch_scheme AND y.mhiscode = a.mhiscode AND x.routecode = a.routecode AND isskintest = 1 ) ";
			sql += " ) AS c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid AND c.prescno = a.prescno AND c.drugcode = a.itemcode ";
			sql += " INNER JOIN mc_dict_route AS d ON b.routematch_scheme = d.match_scheme AND a.routecode = d.routecode AND d.route_type IN (1, 2, 5) ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate< ? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) z ";
			sql += " set x.is_yf = z.is_yf ,x.is_zl= z.is_zl ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode  and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate< ? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate, startdate, enddate, startdate, enddate, startdate,
					enddate, startdate, enddate, startdate, enddate });

			// 10. 5.2.2.1.6 is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt字段处理
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始10.  5.2.2.1.6	is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt字段处理");
			sql = "";
			sql += " INSERT INTO tmp_indtmp_clinicpt(ienddate,mhiscode,caseid,is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt) ";
			sql += " select ienddate,mhiscode,caseid,is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt ";
			sql += " from ( ";
			sql += " SELECT   ienddate, mhiscode, caseid, ";
			sql += " MAX(CASE WHEN drugformtype = 2 AND x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt ";
			sql += " FROM ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, c.drugformtype, d.route_type, d.abbrev ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN mc_dict_route AS d ON d.match_scheme = b.routematch_scheme AND a.routecode = d.routecode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate< ? ";
			sql += " ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z ";
			sql += " where not exists( ";
			sql += " select ienddate,mhiscode,caseid,is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt from tmp_indtmp_clinicpt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode  and z.ienddate = ienddate and ienddate>=? and ienddate< ? ";
			sql += " ) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 11. 5.2.2.1.6 is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt字更新
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始1.  5.2.2.1.6	is_anti_po,is_anti_im,is_anti_iv,is_anti_iv_ivgtt字更新");
			sql = "";
			sql += " update  tmp_indtmp_clinicpt x, ";
			sql += " ( ";
			sql += " SELECT   ienddate, mhiscode, caseid, ";
			sql += " MAX(CASE WHEN drugformtype = 2 AND x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt ";
			sql += " FROM ( ";
			sql += " SELECT a.ienddate, a.mhiscode, a.caseid, c.drugformtype, d.route_type, d.abbrev ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode AND c.is_anti = 1 AND c.antitype IN (1, 2) ";
			sql += " INNER JOIN mc_dict_route AS d ON d.match_scheme = b.routematch_scheme AND a.routecode = d.routecode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate< ? ";
			sql += " ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z ";
			sql += " set x.is_anti_po = z.is_anti_po,x.is_anti_im = z.is_anti_im,x.is_anti_iv = z.is_anti_iv,x.is_anti_iv_ivgtt = z.is_anti_iv_ivgtt ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode  and x.ienddate = z.ienddate and x.ienddate>=? and x.ienddate< ? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 12. 5.2.2.2 tmp_indtmp_costpt预处理过程
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始12.  5.2.2.2	tmp_indtmp_costpt预处理过程");
			sql = "";
			sql += " insert into tmp_indtmp_costpt(ienddate, mhiscode, caseid, cost, drugcost, base_cost, xy_cost, zcy_cost, zyyp_cost, swzj_cost, is_zcy, is_zyyp) ";
			sql += " SELECT x.ienddate, x.mhiscode, x.caseid, x.cost, x.drugcost, y.base_cost, y.xy_cost, y.zcy_cost, y.zyyp_cost, y.swzj_cost, y.is_zcy, y.is_zyyp ";
			sql += " FROM      mc_clinic_patient_medinfo AS x ";
			sql += " LEFT OUTER JOIN ( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, ";
			sql += " SUM(CASE c.is_basedrug WHEN 1 THEN a.cost ELSE NULL END) AS base_cost, ";
			sql += " SUM(CASE c.drugtype WHEN 3 THEN a.cost WHEN 4 THEN a.cost ELSE NULL END) AS xy_cost, ";
			sql += " SUM(CASE c.drugtype WHEN 1 THEN a.cost ELSE NULL END) AS zcy_cost, ";
			sql += " SUM(CASE c.drugtype WHEN 2 THEN a.cost ELSE NULL END) AS zyyp_cost, ";
			sql += " SUM(CASE c.drugtype WHEN 4 THEN a.cost ELSE NULL END) AS swzj_cost, ";
			sql += " MAX(CASE WHEN c.drugtype = 1 AND a.is_use = 1 THEN 1 ELSE 0 END) AS is_zcy, ";
			sql += " MAX(CASE WHEN c.drugtype = 2 AND a.is_use = 1 THEN 1 ELSE 0 END) AS is_zyyp ";
			sql += " FROM       mc_clinic_drugcost_caseid AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode ";
			sql += " where a.ienddate>=? and a.ienddate< ? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) AS y ";
			sql += " ON y.ienddate = x.ienddate AND y.mhiscode = x.mhiscode AND  y.caseid = x.caseid ";
			sql += " Where x.ienddate>=? and x.ienddate< ? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate, startdate, enddate });

			// 13. 5.2.2.3 tmp_indtmp_numpt预处理过程
			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->开始13.  5.2.2.3	tmp_indtmp_numpt预处理过程");
			sql = "";
			sql += " insert into tmp_indtmp_numpt( ienddate, mhiscode, caseid, drugnum, basenum) ";
			sql += " SELECT x.ienddate, x.mhiscode, x.caseid, y.drugnum, z.basenum ";
			sql += " FROM      mc_clinic_patient_medinfo AS x ";
			sql += " LEFT OUTER JOIN ( ";
			sql += " SELECT mods.ienddate, mods.mhiscode, mods.caseid, COUNT(DISTINCT mods.itemcode) AS drugnum ";
			sql += " FROM      mc_clinic_drugcost_caseid AS mods ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mods.mhiscode ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS mdd ON mods.itemcode = mdd.drugcode AND mhmr.drugmatch_scheme = mdd.match_scheme ";
			sql += " LEFT OUTER JOIN mc_dict_route AS mdr ON mdr.routecode = mods.routecode AND mhmr.routematch_scheme = mdr.match_scheme ";
			sql += " WHERE ((mods.is_use = 1) AND (mdd.is_solvent <> 1)) OR  ((mods.is_use = 1) AND (mdr.route_type NOT IN (2, 3)))  ";
			sql += " And mods.ienddate>=? and mods.ienddate< ? GROUP BY mods.ienddate, mods.mhiscode, mods.caseid ";
			sql += " ) AS y ON y.ienddate = x.ienddate AND y.mhiscode = x.mhiscode AND y.caseid = x.caseid and y.ienddate>=? and y.ienddate< ? ";
			sql += " LEFT OUTER JOIN ( ";
			sql += " SELECT   mods.ienddate, mods.mhiscode, mods.caseid, COUNT(DISTINCT mods.itemcode) AS basenum ";
			sql += " FROM      mc_clinic_drugcost_caseid AS mods ";
			sql += " INNER JOIN  mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mods.mhiscode ";
			sql += " INNER JOIN  mc_dict_drug AS mdd ON mhmr.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = mods.itemcode AND mdd.is_basedrug = 1 ";
			sql += " WHERE mods.is_use = 1 and mods.ienddate>=? and mods.ienddate< ? ";
			sql += " GROUP BY mods.ienddate, mods.mhiscode, mods.caseid ";
			sql += " ) AS z ON z.ienddate = x.ienddate AND  z.mhiscode = x.mhiscode AND z.caseid = x.caseid and z.ienddate>=? and z.ienddate< ? ";
			sql += " where x.ienddate>=? and x.ienddate< ? ";
			getJdbcTemplate().update(sql,
					new Object[] { startdate, enddate, startdate, enddate, startdate, enddate, startdate, enddate, startdate, enddate });

			logger.info("合理用药指标统计数据预处理->5.2  门诊病人指标->结束");
		} catch (Exception e) {
			logger.error("合理用药指标统计数据预处理->5.2  门诊病人指标->执行错误：" + e.getMessage(), e);
		}
	}

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
	private void hospPatientIndex(int startdate, int enddate) {
		String sql = "";
		try {
			/** 5.1.2.1 tmp_indtmp_ clinicpres预处理过程 */

			// 清洗数据
			logger.info("5.3.1.2.1	tmp_indtmp_hosppt预处理数据->开始删除数据");

			// 在院门诊指标
			sql = "truncate table tmp_indtmp_hosppt";
			getJdbcTemplate().execute(sql);

			sql = "truncate table tmp_indtmp_hospptcost";
			getJdbcTemplate().execute(sql);

			sql = "truncate table tmp_indtmp_hospptnum";
			getJdbcTemplate().execute(sql);

			for (int i = 1; i <= 12; i++) {
				sql = String.format("delete from tmp_outtmp_hosppt_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

				sql = String.format("delete from tmp_outtmp_hospptcost_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

				sql = String.format("delete from tmp_outtmp_hospptnum_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			}

			logger.info("5.3.1	在院门诊指标");

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

			// =============出院门诊指标==================================================================================
			logger.info("5.3.2	出院门诊指标");
			// 5.3.2.2.1.1 antinum，anticost, antitype1, antitype2, antilevel1, antilevel2,
			// antilevel3预处理数据
			sql = "";
			sql += " insert into tmp_outtmp_hosppt(ienddate,mhiscode,caseid,antinum, anticost,antitype1,antitype2,antilevel1,antilevel2,antilevel3) ";
			sql += " select  a.ienddate, a.mhiscode, a.caseid, COUNT(DISTINCT a.itemcode) AS antinum,  ";
			sql += " SUM(a.cost) AS anticost, MAX(CASE c.antitype WHEN 1 THEN 1 ELSE NULL END) AS antitype1,  ";
			sql += " MAX(CASE c.antitype WHEN 2 THEN 1 ELSE NULL END) AS antitype2, ";
			sql += " MAX(CASE c.antilevel WHEN 1 THEN 1 ELSE NULL END) AS antilevel1,  ";
			sql += " MAX(CASE c.antilevel WHEN 2 THEN 1 ELSE NULL END) AS antilevel2,  ";
			sql += " MAX(CASE c.antilevel WHEN 3 THEN 1 ELSE NULL END) AS antilevel3 ";
			sql += " FROM  mc_outhosp_drugcostdistinct as a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode ";
			sql += " WHERE (a.is_extuse = 0) AND (c.is_anti = 1) AND (c.antitype IN (1, 2)) ";
			sql += " and a.ienddate>=? and a.ienddate<? ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.3.2.2.1.2 unionnum预处理数据
			sql = "";
			sql += " INSERT INTO tmp_outtmp_hosppt(ienddate,mhiscode,caseid,unionnum) ";
			sql += " select  ienddate,mhiscode, caseid, unionnum from ( ";
			sql += " SELECT   ienddate, mhiscode, caseid, MAX(unionnum) AS unionnum FROM (	SELECT   ienddate, mhiscode, caseid, cid, COUNT(DISTINCT ordercode) AS unionnum ";
			sql += " FROM (SELECT   modm.ienddate, modm.mhiscode, modm.caseid, modm.cid, modm2.ordercode FROM  mc_outhosp_drugorder_main AS modm  ";
			sql += " 	INNER JOIN mc_outhosp_drugorder_main AS modm2 ON modm.ienddate = modm2.ienddate AND modm.mhiscode = modm2.mhiscode AND modm.caseid = modm2.caseid AND  ";
			sql += "	modm.startdatetime BETWEEN modm2.startdatetime AND modm2.pa_enddatetime ";
			sql += "	WHERE   (modm.is_out = 0) AND (modm.is_use = 1) AND (modm2.is_out = 0) AND (modm2.is_use = 1)   ";
			sql += " 	AND (modm.ienddate>=? and modm.ienddate<?) ";
			sql += " 	and EXISTS( SELECT   1 AS Expr1 FROM      mc_dict_drug AS a  ";
			sql += " 	INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme 	WHERE   (a.drugcode = modm.ordercode) AND (b.mhiscode = modm.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2)) ";
			sql += " 	) AND   EXISTS ( SELECT   1 AS Expr1  FROM      mc_dict_drug AS a  INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme ";
			sql += " 	WHERE   (a.drugcode = modm2.ordercode) AND (b.mhiscode = modm2.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2))  ";
			sql += " 	) AND  EXISTS (SELECT   1 AS Expr1 FROM  mc_dict_route AS c  ";
			sql += " 	INNER JOIN mc_hospital_match_relation AS d ON c.match_scheme = d.routematch_scheme WHERE   (c.routecode = modm.routecode) AND (d.mhiscode = modm.mhiscode) AND (c.route_type IN (1, 2, 5)) ";
			sql += " 	) AND	EXISTS (SELECT  1 AS Expr1 FROM  mc_dict_route AS c  ";
			sql += " 	INNER JOIN mc_hospital_match_relation AS d ON c.match_scheme = d.routematch_scheme WHERE   (c.routecode = modm2.routecode) AND (d.mhiscode = modm2.mhiscode) AND (c.route_type IN (1, 2, 5))) ";
			sql += " ) AS x GROUP BY ienddate, mhiscode, caseid, cid ";
			sql += " ) AS y GROUP BY ienddate, mhiscode, caseid ) z ";
			sql += " where not exists(select mhiscode,caseid,unionnum from tmp_outtmp_hosppt where z.caseid = caseid and z.mhiscode = mhiscode and z.ienddate = ienddate) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 第二步：
			sql = "";
			sql += " update  tmp_outtmp_hosppt x,( SELECT   ienddate, mhiscode, caseid, MAX(unionnum) AS unionnum FROM     ";
			sql += " ( SELECT   ienddate, mhiscode, caseid, cid, COUNT(DISTINCT ordercode) AS unionnum FROM   ";
			sql += " ( SELECT   modm.ienddate, modm.mhiscode, modm.caseid, modm.cid, modm2.ordercode FROM      mc_outhosp_drugorder_main AS modm  ";
			sql += " INNER JOIN mc_outhosp_drugorder_main AS modm2 ON modm.ienddate = modm2.ienddate AND modm.mhiscode = modm2.mhiscode AND modm.caseid = modm2.caseid AND  ";
			sql += " modm.startdatetime BETWEEN modm2.startdatetime AND modm2.pa_enddatetime ";
			sql += " WHERE   (modm.is_out = 0) AND (modm.is_use = 1) AND (modm2.is_out = 0) AND (modm2.is_use = 1)  ";
			sql += " and (modm.ienddate>=? and modm.ienddate<?) ";
			sql += "  AND EXISTS( SELECT   1 AS Expr1 FROM      mc_dict_drug AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme ";
			sql += " WHERE   (a.drugcode = modm.ordercode) AND (b.mhiscode = modm.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2)) ";
			sql += " ) AND  ";
			sql += "  EXISTS ( SELECT   1 AS Expr1  FROM      mc_dict_drug AS a  INNER JOIN mc_hospital_match_relation AS b ON a.match_scheme = b.drugmatch_scheme ";
			sql += " WHERE   (a.drugcode = modm2.ordercode) AND (b.mhiscode = modm2.mhiscode) AND (a.is_anti = 1) AND (a.antitype IN (1, 2))  ";
			sql += " ) AND  EXISTS ( SELECT   1 AS Expr1 FROM      mc_dict_route AS c  ";
			sql += " INNER JOIN mc_hospital_match_relation AS d ON c.match_scheme = d.routematch_scheme WHERE   (c.routecode = modm.routecode) AND (d.mhiscode = modm.mhiscode) AND (c.route_type IN (1, 2, 5)) ";
			sql += " ) AND  EXISTS ( SELECT   1 AS Expr1 FROM      mc_dict_route AS c ";
			sql += " INNER JOIN mc_hospital_match_relation AS d ON c.match_scheme = d.routematch_scheme WHERE   (c.routecode = modm2.routecode) AND (d.mhiscode = modm2.mhiscode) AND (c.route_type IN (1, 2, 5)) ";
			sql += " ) ) AS x GROUP BY ienddate, mhiscode, caseid, cid ) AS y GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z set x.unionnum = z.unionnum  where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.ienddate=z.ienddate  ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.3.2.2.1.3 is_labpt,is_labtype1预处理数据
			// 第一步：
			sql = "";
			sql += " INSERT INTO tmp_outtmp_hosppt(ienddate,mhiscode,caseid,is_labpt, is_labtype1) ";
			sql += " select  ienddate,mhiscode, caseid, is_labpt, is_labtype1 from  ";
			sql += " (SELECT   moc.ienddate, moc.mhiscode, moc.caseid,  ";
			sql += " MAX(CASE WHEN mdc.is_byx IN (2, 3) THEN 1 ELSE 0 END) AS is_labpt,  1 AS is_labtype1 ";
			sql += " FROM    mc_outhosp_cost AS moc  ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = moc.mhiscode  ";
			sql += " INNER JOIN mc_dict_costitem AS mdc ON mhmr.costitemmatch_scheme = mdc.match_scheme AND mdc.itemcode = moc.itemcode AND mdc.itemtype = 3 AND mdc.is_byx > 0 ";
			sql += " WHERE   (moc.costtype = 3)  ";
			sql += " and (moc.ienddate>=? and moc.ienddate<?) ";
			sql += " GROUP BY moc.ienddate, moc.mhiscode, moc.caseid) z ";
			sql += " where not exists( ";
			sql += " select ienddate,mhiscode,caseid,is_labpt, is_labtype1 from tmp_outtmp_hosppt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode and z.ienddate = ienddate) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 第二步
			sql = "";
			sql += " update  tmp_outtmp_hosppt x,( ";
			sql += " SELECT   moc.ienddate, moc.mhiscode, moc.caseid,  ";
			sql += " MAX(CASE WHEN mdc.is_byx IN (2, 3) THEN 1 ELSE 0 END) AS is_labpt,  1 AS is_labtype1 ";
			sql += " FROM    mc_outhosp_cost AS moc  ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = moc.mhiscode  ";
			sql += " INNER JOIN mc_dict_costitem AS mdc ON mhmr.costitemmatch_scheme = mdc.match_scheme AND mdc.itemcode = moc.itemcode AND mdc.itemtype = 3 AND mdc.is_byx > 0 ";
			sql += " WHERE   (moc.costtype = 3) and (moc.ienddate>=? and moc.ienddate<?) ";
			sql += " GROUP BY moc.ienddate, moc.mhiscode, moc.caseid ";
			sql += " ) z set x.is_labpt = z.is_labpt,x.is_labtype1 = z.is_labtype1 ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.ienddate=z.ienddate  ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.3.2.2.1.4 is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl预处理数据
			// 第一步：
			sql = "";
			sql += " INSERT INTO tmp_outtmp_hosppt(ienddate,mhiscode,caseid,is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl)  ";
			sql += "  select  ienddate,mhiscode,caseid,is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl from (  ";
			sql += "  SELECT   a.ienddate, a.mhiscode, a.caseid,   ";
			sql += " MAX(CASE WHEN a.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf,   ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND  a.antilevel = 2 THEN 1 ELSE 0 END) AS is_xz_zl,   ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND  a.antilevel = 3 THEN 1 ELSE 0 END) AS is_ts_zl,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND a.antilevel = 1 THEN 1 ELSE 0 END) AS is_fxz_zl  ";
			sql += " FROM  (SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode AS drugcode, a.routecode, a.deptcode, d .useway, a.antilevel  ";
			sql += " FROM  ( SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode, a.routecode, a.deptcode, a.doctorcode, a.is_out, a.is_allow,   ";
			sql += " a.startdatetime, a.pa_enddatetime AS enddatetime, a.meditime, a.executetime, d.antitype, d.antilevel, e.route_type  ";
			sql += " FROM       mc_outhosp_drugorder_main AS a   ";
			sql += " INNER JOIN mc_hospital_match_relation AS c ON c.mhiscode = a.mhiscode   ";
			sql += " INNER JOIN mc_dict_drug AS d ON c.drugmatch_scheme = d.match_scheme AND a.ordercode = d.drugcode   ";
			sql += " INNER JOIN mc_dict_route AS e ON c.routematch_scheme = e.match_scheme AND a.routecode = e.routecode  ";
			sql += " WHERE   (d.is_anti = 1) AND (d.antitype IN (1, 2)) AND (a.is_use = 1) AND (e.isskintest <> 1) ) a   ";
			sql += " INNER JOIN pre_outhosp_druguseway d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .cid = a.cid  ";
			sql += " UNION ALL ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode AS drugcode, a.routecode, a.deptcode,  ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway, a.antilevel  ";
			sql += " FROM   ( SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode, a.routecode, a.deptcode, a.doctorcode, a.is_out, a.is_allow,  ";
			sql += " a.startdatetime, a.pa_enddatetime AS enddatetime, a.meditime, a.executetime, d.antitype, d.antilevel, e.route_type  ";
			sql += " FROM       mc_outhosp_drugorder_main AS a  INNER JOIN mc_hospital_match_relation AS c ON c.mhiscode = a.mhiscode   ";
			sql += " INNER JOIN mc_dict_drug AS d ON c.drugmatch_scheme = d.match_scheme AND a.ordercode = d.drugcode   ";
			sql += " INNER JOIN mc_dict_route AS e ON c.routematch_scheme = e.match_scheme AND a.routecode = e.routecode  ";
			sql += " WHERE   (d.is_anti = 1) AND (d.antitype IN (1, 2)) AND (a.is_use = 1) AND (e.isskintest <> 1)  ";
			sql += " ) a  INNER JOIN ( SELECT   a.ienddate, a.mhiscode, a.caseid, IFNULL(b.distype, 0) AS distype1, IFNULL(c.distype, 0) AS distype2 FROM mc_outhosp_patient_baseinfo a   ";
			sql += " LEFT JOIN ( SELECT   x.ienddate, x.mhiscode, x.caseid, 1 AS distype FROM      mc_outhosp_disease x   ";
			sql += " INNER JOIN mc_hospital_match_relation y ON y.mhiscode = x.mhiscode   ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND  x.discode = z.discode  ";
			sql += " WHERE   z.dis_type = 1  GROUP BY x.ienddate, x.mhiscode, x.caseid  ";
			sql += " ) b ON b.ienddate = a.ienddate AND b.mhiscode = a.mhiscode AND b.caseid = a.caseid  ";
			sql += " LEFT JOIN ( SELECT   x.ienddate, x.mhiscode, x.caseid, 2 AS distype FROM      mc_outhosp_disease x   ";
			sql += " INNER JOIN mc_hospital_match_relation y ON y.mhiscode = x.mhiscode   ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode  ";
			sql += " WHERE   z.dis_type = 2 GROUP BY x.ienddate, x.mhiscode, x.caseid  ";
			sql += " ) c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid  ";
			sql += " ) d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid  ";
			sql += " 	WHERE   NOT EXISTS ( SELECT 1 FROM      pre_outhosp_druguseway WHERE   ienddate = a.ienddate AND mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid)  ";
			sql += " ) a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode   ";
			sql += " INNER JOIN mc_dict_route AS e ON b.routematch_scheme = e.match_scheme AND e.routecode = a.routecode  ";
			sql += " WHERE   (e.route_type IN (1, 2, 5))  ";
			sql += " and (a.ienddate>=? and a.ienddate<?)  ";
			sql += " GROUP BY a.ienddate, a.mhiscode, a.caseid ) z  ";
			sql += " where not exists(  ";
			sql += " select ienddate,mhiscode,caseid,is_yf, is_zl,is_xz_zl,is_ts_zl,is_fxz_zl from tmp_outtmp_hosppt where z.caseid = caseid and z.mhiscode = mhiscode and z.ienddate = ienddate)  ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 第二步
			sql = "";
			sql += " update  tmp_outtmp_hosppt x,( ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid,  ";
			sql += "  MAX(CASE WHEN a.useway IN (1, 3, 5) THEN 1 ELSE 0 END) AS is_yf,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) THEN 1 ELSE 0 END) AS is_zl, ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND  a.antilevel = 2 THEN 1 ELSE 0 END) AS is_xz_zl,  ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND  a.antilevel = 3 THEN 1 ELSE 0 END) AS is_ts_zl, ";
			sql += " MAX(CASE WHEN a.useway IN (2, 4, 5) AND a.antilevel = 1 THEN 1 ELSE 0 END) AS is_fxz_zl ";
			sql += " FROM  (SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode AS drugcode, a.routecode, a.deptcode, d .useway, a.antilevel ";
			sql += " FROM   ( SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode, a.routecode, a.deptcode, a.doctorcode, a.is_out, a.is_allow,  ";
			sql += " a.startdatetime, a.pa_enddatetime AS enddatetime, a.meditime, a.executetime, d.antitype, d.antilevel, e.route_type ";
			sql += " FROM       mc_outhosp_drugorder_main AS a  INNER JOIN mc_hospital_match_relation AS c ON c.mhiscode = a.mhiscode  ";
			sql += " INNER JOIN mc_dict_drug AS d ON c.drugmatch_scheme = d.match_scheme AND a.ordercode = d.drugcode  ";
			sql += " INNER JOIN mc_dict_route AS e ON c.routematch_scheme = e.match_scheme AND a.routecode = e.routecode ";
			sql += " WHERE   (d.is_anti = 1) AND (d.antitype IN (1, 2)) AND (a.is_use = 1) AND (e.isskintest <> 1) ) a  ";
			sql += " INNER JOIN pre_outhosp_druguseway d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid AND d .cid = a.cid ";
			sql += " UNION ALL ";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode AS drugcode, a.routecode, a.deptcode, ";
			sql += " CASE WHEN antitype = 1 AND distype1 = 1 THEN 2 WHEN antitype = 2 AND distype2 = 2 THEN 2 ELSE 1 END AS useway, a.antilevel ";
			sql += " FROM  ( SELECT   a.ienddate, a.mhiscode, a.caseid, a.cid, a.ordercode, a.routecode, a.deptcode, a.doctorcode, a.is_out, a.is_allow,  ";
			sql += " a.startdatetime, a.pa_enddatetime AS enddatetime, a.meditime, a.executetime, d.antitype, d.antilevel, e.route_type ";
			sql += " FROM       mc_outhosp_drugorder_main AS a ";
			sql += " INNER JOIN mc_hospital_match_relation AS c ON c.mhiscode = a.mhiscode  ";
			sql += " INNER JOIN mc_dict_drug AS d ON c.drugmatch_scheme = d.match_scheme AND a.ordercode = d.drugcode  ";
			sql += " INNER JOIN mc_dict_route AS e ON c.routematch_scheme = e.match_scheme AND a.routecode = e.routecode ";
			sql += " WHERE   (d.is_anti = 1) AND (d.antitype IN (1, 2)) AND (a.is_use = 1) AND (e.isskintest <> 1) )  a  ";
			sql += " INNER JOIN (";
			sql += " SELECT   a.ienddate, a.mhiscode, a.caseid, IFNULL(b.distype, 0) AS distype1, IFNULL(c.distype, 0) AS distype2 FROM mc_outhosp_patient_baseinfo a ";
			sql += " LEFT JOIN ( SELECT   x.ienddate, x.mhiscode, x.caseid, 1 AS distype FROM      mc_outhosp_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON y.mhiscode = x.mhiscode  ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND  x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 1  GROUP BY x.ienddate, x.mhiscode, x.caseid ";
			sql += " ) b ON b.ienddate = a.ienddate AND b.mhiscode = a.mhiscode AND b.caseid = a.caseid  ";
			sql += " LEFT JOIN ( SELECT   x.ienddate, x.mhiscode, x.caseid, 2 AS distype FROM      mc_outhosp_disease x  ";
			sql += " INNER JOIN mc_hospital_match_relation y ON y.mhiscode = x.mhiscode  ";
			sql += " INNER JOIN mc_dict_disease z ON y.dismatch_scheme = z.match_scheme AND x.discode = z.discode ";
			sql += " WHERE   z.dis_type = 2 GROUP BY x.ienddate, x.mhiscode, x.caseid ";
			sql += " ) c ON c.ienddate = a.ienddate AND c.mhiscode = a.mhiscode AND c.caseid = a.caseid ";
			sql += " ) d ON d .ienddate = a.ienddate AND d .mhiscode = a.mhiscode AND d .caseid = a.caseid ";

			sql += " WHERE NOT EXISTS ( SELECT 1 FROM pre_outhosp_druguseway WHERE ienddate = a.ienddate AND mhiscode = a.mhiscode AND caseid = a.caseid AND cid = a.cid ) ) a ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode  ";
			sql += " INNER JOIN mc_dict_route AS e ON b.routematch_scheme = e.match_scheme AND e.routecode = a.routecode ";
			sql += " WHERE   (e.route_type IN (1, 2, 5)) GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) z set x.is_yf = z.is_yf,x.is_zl = z.is_zl,x.is_xz_zl = z.is_xz_zl,x.is_ts_zl=z.is_ts_zl,x.is_fxz_zl=z.is_fxz_zl ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.ienddate=z.ienddate  ";
			sql += " and (x.ienddate>=? and x.ienddate<?)  ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.3.2.2.1.5 is_opr预处理数据
			sql = "";
			sql += " INSERT INTO tmp_outtmp_hosppt(ienddate,mhiscode,caseid,is_opr) ";
			sql += " select ienddate,mhiscode, caseid, is_opr from ( SELECT moo.ienddate, moo.mhiscode, moo.caseid, MAX(CASE WHEN mlot.typecode IS NULL THEN 1 ELSE NULL END) AS is_opr ";
			sql += " FROM mc_outhosp_operation AS moo  ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = moo.mhiscode ";
			sql += " LEFT OUTER JOIN mc_link_operation_type AS mlot ON mlot.operationcode = moo.operationcode AND mhmr.oprmatch_scheme = mlot.match_scheme AND mlot.typecode = 0  ";
			sql += " where (moo.ienddate>=? and moo.ienddate<?)  ";
			sql += " GROUP BY moo.ienddate, moo.mhiscode, moo.caseid) z ";
			sql += " where not exists(select ienddate,mhiscode,caseid,is_opr from tmp_outtmp_hosppt where z.caseid = caseid and z.mhiscode = mhiscode and z.ienddate = ienddate) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 第二步：
			sql = "";
			sql += " update tmp_outtmp_hosppt x,( ";
			sql += " SELECT moo.ienddate, moo.mhiscode, moo.caseid, MAX(CASE WHEN mlot.typecode IS NULL THEN 1 ELSE NULL END) AS is_opr ";
			sql += " FROM mc_outhosp_operation AS moo  ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = moo.mhiscode  ";
			sql += " LEFT OUTER JOIN mc_link_operation_type AS mlot ON mlot.operationcode = moo.operationcode AND mhmr.oprmatch_scheme = mlot.match_scheme AND mlot.typecode = 0 ";
			sql += " where (moo.ienddate>=? and moo.ienddate<?)  ";
			sql += " GROUP BY moo.ienddate, moo.mhiscode, moo.caseid ";
			sql += " ) z set x.is_opr = z.is_opr ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.ienddate=z.ienddate  ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.3.2.2.1.6 is_anti_po,
			// is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt预处理数据
			// 第一步：
			sql = "";
			sql += " INSERT INTO tmp_outtmp_hosppt(ienddate,mhiscode,caseid,is_anti_po, is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt) ";
			sql += " select  ienddate,mhiscode,caseid,is_anti_po, is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt from  ";
			sql += "  (SELECT   ienddate, mhiscode, caseid, ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 2 AND  x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND  antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt, ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_anti_ivgtt,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_anti_formtype1, ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_formtype1,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_ivgtt ";
			sql += " FROM  (SELECT   a.ienddate, a.mhiscode, a.caseid, c.is_anti, c.antitype, c.drugformtype, d.route_type, d.abbrev ";
			sql += " FROM  mc_outhosp_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode  ";
			sql += " INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode  ";
			sql += " INNER JOIN mc_dict_route AS d ON d.match_scheme = b.routematch_scheme AND a.routecode = d.routecode ";
			sql += " WHERE   (a.is_use = 1) AND (c.drugformtype IN (1, 2)) and a.ienddate>=? and a.ienddate<? ";
			sql += " ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid) z ";
			sql += " where not exists( ";
			sql += " select ienddate,mhiscode,caseid,is_anti_po, is_anti_im,is_anti_iv,is_anti_iv_ivgtt,is_anti_ivgtt,is_anti_formtype1,is_formtype1,is_ivgtt from tmp_outtmp_hosppt ";
			sql += " where z.caseid = caseid and z.mhiscode = mhiscode and z.ienddate = ienddate) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 第二步：
			sql = "";
			sql += " update  tmp_outtmp_hosppt x,( ";
			sql += " SELECT   ienddate, mhiscode, caseid,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 2 AND  x.route_type = 1 THEN 1 ELSE NULL END) AS is_anti_po,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'im' THEN 1 ELSE NULL END) AS is_anti_im,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND  antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'iv' THEN 1 ELSE NULL END) AS is_anti_iv,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev IN ('iv', 'ivgtt') THEN 1 ELSE NULL END) AS is_anti_iv_ivgtt, ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_anti_ivgtt,  ";
			sql += " MAX(CASE WHEN is_anti = 1 AND antitype IN (1, 2) AND drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_anti_formtype1,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.route_type = 2 THEN 1 ELSE NULL END) AS is_formtype1,  ";
			sql += " MAX(CASE WHEN drugformtype = 1 AND x.abbrev = 'ivgtt' THEN 1 ELSE NULL END) AS is_ivgtt ";
			sql += " FROM   ( SELECT   a.ienddate, a.mhiscode, a.caseid, c.is_anti, c.antitype, c.drugformtype, d.route_type, d.abbrev ";
			sql += " FROM  mc_outhosp_drugcost_caseid AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON a.mhiscode = b.mhiscode  ";
			sql += " INNER JOIN mc_dict_drug AS c ON c.match_scheme = b.drugmatch_scheme AND c.drugcode = a.itemcode  ";
			sql += " INNER JOIN mc_dict_route AS d ON d.match_scheme = b.routematch_scheme AND a.routecode = d.routecode ";
			sql += " WHERE   (a.is_use = 1) AND (c.drugformtype IN (1, 2))  ) AS x ";
			sql += " GROUP BY ienddate, mhiscode, caseid ";
			sql += " ) z set x.is_anti_po = z.is_anti_po,x.is_anti_im = z.is_anti_im,x.is_anti_iv= z.is_anti_iv,x.is_anti_iv_ivgtt=z.is_anti_iv_ivgtt,x.is_anti_ivgtt=z.is_anti_ivgtt,x.is_anti_formtype1=z.is_anti_formtype1,x.is_formtype1=z.is_formtype1,x.is_ivgtt=z.is_ivgtt ";
			sql += " where x.caseid = z.caseid and x.mhiscode = z.mhiscode and x.ienddate=z.ienddate  ";
			sql += " and x.ienddate>=? and x.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.3.2.2.2 tmp_outtmp_hospptcost数据预处理
			sql = "";
			sql += " insert into tmp_outtmp_hospptcost(ienddate, mhiscode, caseid, cost, drugcost, base_cost, xy_cost, zcy_cost, zyyp_cost,swzj_cost) ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.cost, x.drugcost, y.base_cost, y.xy_cost, y.zcy_cost, y.zyyp_cost,y.swzj_cost ";
			sql += " FROM      mc_outhosp_patient_medinfo AS x  ";
			sql += " LEFT OUTER JOIN ( SELECT   a.ienddate, a.mhiscode, a.caseid,  ";
			sql += " (CASE c.is_basedrug WHEN 1 THEN a.cost ELSE NULL END)AS base_cost,  ";
			sql += " SUM(CASE c.drugtype WHEN 3 THEN a.cost WHEN 4 THEN a.cost ELSE NULL END) AS xy_cost,  ";
			sql += " SUM(CASE c.drugtype WHEN 1 THEN a.cost ELSE NULL END) AS zcy_cost,  ";
			sql += "  SUM(CASE c.drugtype WHEN 2 THEN a.cost ELSE NULL END) AS zyyp_cost,  ";
			sql += "  SUM(CASE c.drugtype WHEN 4 THEN a.cost ELSE NULL END) AS swzj_cost ";
			sql += "  FROM      mc_outhosp_drugcostdistinct AS a  ";
			sql += " INNER JOIN mc_hospital_match_relation AS b ON b.mhiscode = a.mhiscode  ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS c ON b.drugmatch_scheme = c.match_scheme AND c.drugcode = a.itemcode ";
			sql += "  GROUP BY a.ienddate, a.mhiscode, a.caseid ";
			sql += " ) AS y ON y.ienddate = x.ienddate AND y.mhiscode = x.mhiscode AND y.caseid = x.caseid ";
			sql += " and x.ienddate>=? and x.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.3.2.2.3 tmp_outtmp_hospptnum数据预处理
			sql = "";
			sql += " insert into tmp_outtmp_hospptnum( ienddate, mhiscode, caseid, drugnum, basenum) ";
			sql += " SELECT   x.ienddate, x.mhiscode, x.caseid, x.drugnum, y.basenum ";
			sql += " FROM  (	SELECT   ienddate, mhiscode, caseid,  ";
			sql += " COUNT(DISTINCT itemcode) AS drugnum FROM      mc_outhosp_drugcostdistinct AS mods GROUP BY ienddate, mhiscode, caseid) AS x  ";
			sql += " LEFT OUTER JOIN(SELECT   mods.ienddate, mods.mhiscode, mods.caseid, COUNT(DISTINCT mods.itemcode) AS basenum ";
			sql += " FROM      mc_outhosp_drugcostdistinct AS mods  ";
			sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mods.mhiscode  ";
			sql += " LEFT OUTER JOIN mc_dict_drug AS mdd ON mhmr.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = mods.itemcode ";
			sql += " WHERE   (mdd.is_basedrug = 1) ";
			sql += " GROUP BY mods.ienddate, mods.mhiscode, mods.caseid ";
			sql += " ) AS y ON y.ienddate = x.ienddate AND y.mhiscode = x.mhiscode AND y.caseid = x.caseid";
			sql += " and x.ienddate>=? and x.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			logger.info("5.3  住院病人指标->结束");
		} catch (Exception e) {
			logger.error("5.3  住院病人指标->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  kjy_use_num </li>
	 * <li>功能描述：5.4  抗菌药物使用量 </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月27日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void kjy_use_num(int startdate, int enddate) {
		String sql = "";
		try {

			// for (int i = 1; i <= 12; i++) {
			// 根据日期删除数据
			sql = "delete from mc_indtmp_ddds where (ienddate>=? and ienddate<?) or selecttype=3 ";
			// sql = String.format("delete from mc_indtmp_ddds_%02d where (ienddate>=? and
			// ienddate<?) or selecttype=3 ", i);
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			// }

			// 5.4.2.1 门急诊数据预处理
			sql = "";
			sql += " insert into mc_indtmp_ddds(drugindex, match_scheme, mhiscode, itemunit, itemnum, costtime, deptcode, medgroupcode, doctorcode, route_type, selecttype,ienddate) ";
			sql += " SELECT mcdc.drugindex, mhmr.drugmatch_scheme as match_scheme, mcdc.mhiscode, mcdc.itemunit, mcdc.itemnum, mcdc.costtime, mcdc.deptcode, mcdc.medgroupcode, mcdc.doctorcode, mdr.route_type, 1 as selecttype,mcdc.ienddate  ";
			sql += " FROM   mc_clinic_drugcost_costtime mcdc ";
			sql += " LEFT JOIN mc_clinic_caseid_ienddate mcci ON mcci.caseid = mcdc.caseid 	AND mcci.mhiscode = mcdc.mhiscode ";
			sql += " LEFT JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mcdc.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON  mdd.drugcode = mcdc.itemcode AND mdd.match_scheme = mhmr.drugmatch_scheme ";
			sql += " LEFT JOIN mc_dict_route AS mdr ON mdr.routecode = mcdc.routecode AND mdr.match_scheme = mhmr.routematch_scheme ";
			sql += " WHERE  mdd.is_anti = 1  AND mdd.antitype BETWEEN 1 AND 2   ";
			sql += " and mcdc.ienddate>=? and mcdc.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.4.2.2 出院数据预处理
			sql = "";
			sql += " insert into mc_indtmp_ddds(drugindex,match_scheme,mhiscode,itemunit,itemnum,costtime,deptcode,medgroupcode,doctorcode,route_type,selecttype,ienddate) ";
			sql += " SELECT modc.drugindex, mhmr.drugmatch_scheme as match_scheme, modc.mhiscode, modc.itemunit, modc.itemnum, modc.costtime, modc.deptcode, modc.medgroupcode, modc.doctorcode, mdr.route_type, 2 as selecttype,modc.ienddate  ";
			sql += " FROM   mc_outhosp_drugcost_costtime modc ";
			sql += " LEFT JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = modc.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON  mdd.drugcode = modc.itemcode ";
			sql += " AND mdd.match_scheme = mhmr.drugmatch_scheme ";
			sql += " LEFT JOIN mc_dict_route AS mdr ON mdr.routecode = modc.routecode AND mdr.match_scheme = mhmr.routematch_scheme ";
			sql += " WHERE  mdd.is_anti = 1  AND mdd.antitype BETWEEN 1 AND 2  ";
			sql += " and modc.ienddate>=? and modc.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 5.4.2.3 在院数据预处理
			sql = "";
			sql += " insert into mc_indtmp_ddds(drugindex,match_scheme,mhiscode,itemunit,itemnum,costtime,deptcode,medgroupcode,doctorcode,route_type,selecttype,ienddate) ";
			sql += " SELECT midc.drugindex, mhmr.drugmatch_scheme as match_scheme, midc.mhiscode, midc.itemunit, midc.itemnum, midc.costtime, midc.deptcode, midc.medgroupcode, midc.doctorcode, mdr.route_type, 3 as selecttype,'19000101' as ienddate ";
			sql += " FROM   mc_inhosp_drugcost_costtime midc ";
			sql += " LEFT JOIN mc_hospital_match_relation AS mhmr ";
			sql += " ON mhmr.mhiscode = midc.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON  mdd.drugcode = midc.itemcode AND mdd.match_scheme = mhmr.drugmatch_scheme ";
			sql += " LEFT JOIN mc_dict_route AS mdr ON mdr.routecode = midc.routecode AND mdr.match_scheme = mhmr.routematch_scheme ";
			sql += " WHERE  mdd.is_anti = 1 AND mdd.antitype BETWEEN 1 AND 2   ";
			getJdbcTemplate().update(sql);

			logger.info("5.4  抗菌药物使用量->结束");
		} catch (Exception e) {
			logger.error("5.4  抗菌药物使用量->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  indtmpSyqd </li>
	 * <li>功能描述：5.5	抗菌药物使用强度 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年8月8日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void indtmpSyqd(int startdate, int enddate) {
		String sql = "";
		try {

			// for (int i = 1; i <= 12; i++) {
			// 根据日期删除数据
			sql = "delete from mc_indtmp_syqd where (ienddate>=? and ienddate<?) ";
			// sql = String.format("delete from mc_indtmp_ddds_%02d where (ienddate>=? and
			// ienddate<?) or selecttype=3 ", i);
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			// }

			// 5.5 抗菌药物使用强度
			sql = "";
			sql += " insert into mc_indtmp_syqd (mhiscode,drugindex,match_scheme,itemunit,itemnum,ienddate,hospdays,deptcode,is_out,iage,hospday,route_type)";
			sql += " SELECT modc.mhiscode, modc.drugindex, mhmr.drugmatch_scheme as match_scheme, modc.itemunit, modc.itemnum, 0 as hospdays, modc.ienddate, modc.deptcode, modc.is_out, mopm.iage, mopm.hospday, mdr.route_type";
			sql += " FROM mc_outhosp_drugcost_caseid modc LEFT JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = modc.mhiscode ";
			sql += " LEFT JOIN mc_outhosp_patient_medinfo AS mopm ON mopm.ienddate = modc.ienddate AND mopm.caseid = modc.caseid AND mopm.mhiscode = modc.mhiscode";
			sql += " LEFT JOIN mc_dict_route AS mdr ON mdr.routecode = modc.routecode AND mdr.match_scheme = mhmr.routematch_scheme ";
			sql += " where modc.ienddate>=? and modc.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			logger.info("抗菌药物使用强度->结束");
		} catch (Exception e) {
			logger.error("抗菌药物使用强度->执行错误：" + e.getMessage(), e);
		}
	}

}
