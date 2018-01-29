package com.fourfire.dealother;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.medicom.modules.persistence.JdbcGenericMysqlDao;

/**
 * 
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  UnAuthDrugStatDao </li>
 * <li>类描述： 越权用药等 统计，文档中7.*  </li>
 * <li>创建人：jy </li>
 * <li>创建时间：2017年6月23日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Repository
public class UnAuthDrugStatDao extends JdbcGenericMysqlDao {

	private final Logger logger = LoggerFactory.getLogger(UnAuthDrugStatDao.class);

	public void deal(String startdate, String enddate) {
		try {
			// 清洗数据
			int startdateInt = Integer.parseInt(startdate.replace("-", ""));
			int enddateInt = Integer.parseInt(enddate.replace("-", ""));

			logger.info("[越权冗余处理]--开始");

			// 7.1.1 不合理越权明细表

			// 2） 前置处理语句（审查数据产生后即处理）
			// 在院（pharm_screenresults_hosp）审查结果表：
			dealPharmScreenresultsHosp();

			// 出院（pharm_screenresults_hosp）审查结果表
			dealharmcreenresultsosp();

			// 门诊（pharm_screenresults_clinic）审查结果表：
			dealPharmScreenresultsClinic();

			logger.info("[越权冗余处理]--完毕");

			// 7.3 基本药物使用情况统计表(tmp_basedrug_report)
			tmp_basedrug_report(startdateInt, enddateInt);

		} catch (Exception ex) {
			log.error("7.x 越权用药等 统计", ex);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  dealPharmScreenresultsClinic </li>
	 * <li>功能描述：门诊（pharm_screenresults_clinic）审查结果表 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月27日 </li>
	 * </ul>
	 */
	private void dealPharmScreenresultsClinic() {
		logger.info("[越权冗余处理]门诊（pharm_screenresults_clinic）审查结果表->开始");
		String sql = "";

		sql += " update pharm_screenresults tmp1 join ( ";

		sql += " SELECT mdrugm.*, CASE WHEN mddoctor.doctorname = '' OR mddoctor.doctorname IS NULL THEN '未知医生' ELSE mddoctor.doctorname END AS doctorname, ";
		sql += "  CASE WHEN mddept.deptname = '' OR mddept.deptname IS NULL THEN '未知科室' ELSE mddept.deptname END AS deptname, CASE WHEN mddoctor.ilevel = 1 THEN '临床医师' WHEN mddoctor.ilevel = 2 THEN '主治医师' WHEN mddoctor.ilevel = 3 THEN '副主任医师' WHEN mddoctor.ilevel = 4 THEN '主任医师' ELSE '其它' END AS ilevel ";
		sql += " FROM (SELECT DISTINCT mcpm.clinicno AS case_no, mcpm.mhiscode, mcpm.caseid, 2 AS tagtype, mdrugm.startdatetime ";
		sql += " , mcpm.patientname, phsrhosp.drug_unique_code AS drugcode, phsrhosp.drugname, mdrugm.doctorcode, mdrugm.deptcode ";
		sql += " , phsrhosp.modulename, phsrhosp.warning, CASE WHEN mddrug.antilevel = 1 THEN '非限制' WHEN mddrug.antilevel = 2 THEN '限制' WHEN mddrug.antilevel = 3 THEN '特殊' ELSE '其它' END AS antilevel, mddrug.classid, mddrug.jdmtype ";
		sql += " , mddrug.is_anti, mddrug.antitype, mdrugm.is_allow,mhmr.deptmatch_scheme,mhmr.doctormatch_scheme,mhmr.drugmatch_scheme  ";
		// sql += " FROM (SELECT phsrhosp.*, mociend.ienddate FROM pharm_screenresults phsrhosp ";
		sql += " FROM (SELECT phsrhosp.* FROM pharm_screenresults phsrhosp ";
		sql += " INNER JOIN mc_clinic_caseid_ienddate mociend ON phsrhosp.caseid = mociend.caseid AND phsrhosp.mhiscode = mociend.mhiscode ";
		sql += " ) phsrhosp ";
		sql += " INNER JOIN mc_clinic_drugorder_main mdrugm ON phsrhosp.caseid = mdrugm.caseid AND phsrhosp.ienddate = mdrugm.ienddate AND phsrhosp.mhiscode = mdrugm.mhiscode AND phsrhosp.cid = mdrugm.cid ";
		sql += " INNER JOIN mc_clinic_patient_medinfo mcpm ON phsrhosp.caseid = mcpm.caseid AND phsrhosp.mhiscode = mcpm.mhiscode AND phsrhosp.ienddate = mcpm.ienddate ";
		sql += " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = mcpm.mhiscode ";
		sql += " LEFT JOIN mc_dict_drug mddrug ON phsrhosp.drug_unique_code = mddrug.drugcode 	AND mhmr.drugmatch_scheme = mddrug.match_scheme ";
		// sql += " WHERE phsrhosp.moduleid IN (118, 119, 120, 172) AND phsrhosp.patstatus IN (2, 3)
		// AND mcpm.mhiscode IN (0) ";
		sql += " ) mdrugm ";
		sql += " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = mdrugm.mhiscode ";
		sql += " LEFT JOIN mc_dict_doctor mddoctor ON mdrugm.doctorcode = mddoctor.doctorcode AND mddoctor.match_scheme = mhmr.doctormatch_scheme ";
		sql += " LEFT JOIN mc_dict_dept mddept ON mdrugm.deptcode = mddept.deptcode AND mddept.match_scheme = mhmr.deptmatch_scheme ";
		sql += " ORDER BY mdrugm.mhiscode, mdrugm.startdatetime ";

		sql += " )tmp2  ";
		sql += " on tmp1.caseid=tmp2.caseid and tmp1.mhiscode = tmp2.mhiscode ";
		sql += " set  tmp1.case_no = tmp2.case_no, tmp1.patientname = tmp2.patientname, tmp1.deptname= tmp2.deptname, tmp1.doctorname= tmp2.doctorname, tmp1.ilevel= tmp2.ilevel, tmp1.classid= tmp2.classid, tmp1.jdmtype= tmp2.jdmtype, tmp1.is_anti= tmp2.is_anti, tmp1.antitype= tmp2.antitype, tmp1.antilevel= tmp2.antilevel, tmp1.startdatetime= tmp2.startdatetime, tmp1.is_allow= tmp2.is_allow ";
		sql += ",tmp1.deptmatch_scheme=tmp2.deptmatch_scheme,tmp1.doctormatch_scheme=tmp2.doctormatch_scheme,tmp1.drugmatch_scheme=tmp2.drugmatch_scheme";
		sql += " where tmp1.case_no is null or tmp1.case_no='' ";

		getJdbcTemplate().update(sql);

		logger.info("[越权冗余处理]门诊（pharm_screenresults_clinic）审查结果表->结束");
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  dealharmcreenresultsosp </li>
	 * <li>功能描述： 出院（pharm_screenresults_hosp）审查结果表：</li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月26日 </li>
	 * </ul>
	 */
	private void dealharmcreenresultsosp() {
		logger.info("[越权冗余处理] 出院（pharm_screenresults_hosp）审查结果表->开始");
		String sql = "";
		sql += " update pharm_screenresults tmp1 join ( ";
		sql += " SELECT mdrugm.*, CASE WHEN mddoctor.doctorname = '' OR mddoctor.doctorname IS NULL THEN '未知医生' ELSE mddoctor.doctorname END AS doctorname,  ";
		sql += " CASE WHEN mddept.deptname = '' OR mddept.deptname IS NULL THEN '未知科室' ELSE mddept.deptname END AS deptname, CASE WHEN mddoctor.ilevel = 1 THEN '临床医师' WHEN mddoctor.ilevel = 2 THEN '主治医师' WHEN mddoctor.ilevel = 3 THEN '副主任医师' WHEN mddoctor.ilevel = 4 THEN '主任医师' ELSE '其它' END AS ilevel ";
		sql += " FROM (SELECT DISTINCT mcpm.hospitalno AS case_no, mcpm.mhiscode, mcpm.caseid, 0 AS tagtype, mdrugm.startdatetime ";
		sql += " , mcpm.patientname, phsrhosp.drug_unique_code AS drugcode, phsrhosp.drugname, mdrugm.doctorcode, mdrugm.deptcode ";
		sql += " , phsrhosp.modulename, phsrhosp.warning, CASE WHEN mddrug.antilevel = 1 THEN '非限制' WHEN mddrug.antilevel = 2 THEN '限制' WHEN mddrug.antilevel = 3 THEN '特殊' ELSE '其它' END AS antilevel, mddrug.classid, mddrug.jdmtype ";
		sql += " , mddrug.is_anti, mddrug.antitype, mdrugm.is_allow,mhmr.deptmatch_scheme,mhmr.doctormatch_scheme,mhmr.drugmatch_scheme ";
		// sql += " FROM (SELECT phsrhosp.*, mociend.ienddate FROM pharm_screenresults phsrhosp ";
		sql += " FROM (SELECT phsrhosp.* FROM pharm_screenresults phsrhosp ";
		sql += " 	INNER JOIN mc_outhosp_caseid_ienddate mociend ON phsrhosp.caseid = mociend.caseid AND phsrhosp.mhiscode = mociend.mhiscode ";
		sql += " ) phsrhosp ";
		sql += " INNER JOIN mc_outhosp_drugorder_main mdrugm ON phsrhosp.caseid = mdrugm.caseid 	AND phsrhosp.ienddate = mdrugm.ienddate 	AND phsrhosp.mhiscode = mdrugm.mhiscode AND phsrhosp.cid = mdrugm.cid ";
		sql += " INNER JOIN mc_outhosp_patient_medinfo mcpm ON phsrhosp.caseid = mcpm.caseid AND phsrhosp.ienddate = mcpm.ienddate AND phsrhosp.mhiscode = mcpm.mhiscode ";
		sql += " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = mcpm.mhiscode ";
		sql += " LEFT JOIN mc_dict_drug mddrug ON phsrhosp.drug_unique_code = mddrug.drugcode AND mhmr.drugmatch_scheme = mddrug.match_scheme ";
		// sql += " WHERE phsrhosp.moduleid IN (118, 119, 120, 172) AND phsrhosp.patstatus = 0 AND
		// mcpm.mhiscode IN (0) ";
		sql += " ) mdrugm ";
		sql += " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = mdrugm.mhiscode ";
		sql += " LEFT JOIN mc_dict_doctor mddoctor ON mdrugm.doctorcode = mddoctor.doctorcode 	AND mddoctor.match_scheme = mhmr.doctormatch_scheme ";
		sql += " LEFT JOIN mc_dict_dept mddept ON mdrugm.deptcode = mddept.deptcode 	AND mddept.match_scheme = mhmr.deptmatch_scheme ";
		sql += " ORDER BY mdrugm.mhiscode, mdrugm.startdatetime ";

		sql += " )tmp2  ";
		sql += " on tmp1.caseid=tmp2.caseid and tmp1.mhiscode = tmp2.mhiscode ";
		sql += " set  tmp1.case_no = tmp2.case_no, tmp1.patientname = tmp2.patientname, tmp1.deptname= tmp2.deptname, tmp1.doctorname= tmp2.doctorname, tmp1.ilevel= tmp2.ilevel, tmp1.classid= tmp2.classid, tmp1.jdmtype= tmp2.jdmtype, tmp1.is_anti= tmp2.is_anti, tmp1.antitype= tmp2.antitype, tmp1.antilevel= tmp2.antilevel, tmp1.startdatetime= tmp2.startdatetime, tmp1.is_allow= tmp2.is_allow ";
		sql += ",tmp1.deptmatch_scheme=tmp2.deptmatch_scheme,tmp1.doctormatch_scheme=tmp2.doctormatch_scheme,tmp1.drugmatch_scheme=tmp2.drugmatch_scheme";

		sql += " where tmp1.case_no is null or tmp1.case_no='' ";

		getJdbcTemplate().update(sql);

		logger.info("[越权冗余处理] 出院（pharm_screenresults_hosp）审查结果表->结束");
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  dealPharmScreenresultsHosp </li>
	 * <li>功能描述：1)在院（pharm_screenresults_hosp）审查结果表： </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月26日 </li>
	 * </ul>
	 */
	private void dealPharmScreenresultsHosp() {
		logger.info("[越权冗余处理] 1)在院（pharm_screenresults_hosp）审查结果表->开始");
		String sql = "";
		sql += " update pharm_screenresults tmp1 join ( SELECT mdrugm.*, CASE WHEN( mddoctor.doctorname = '' OR mddoctor.doctorname IS NULL )THEN '未知医生' ELSE mddoctor.doctorname END AS doctorname, CASE WHEN( mddept.deptname = '' OR mddept.deptname IS NULL )THEN '未知科室' ELSE mddept.deptname END AS deptname, CASE WHEN mddoctor.ilevel = 1 THEN '临床医师' WHEN mddoctor.ilevel = 2 THEN '主治医师' WHEN mddoctor.ilevel = 3 THEN '副主任医师' WHEN mddoctor.ilevel = 4 THEN '主任医师' ELSE '其它' END AS ilevel ";

		sql += " FROM ( ";
		sql += " 	SELECT DISTINCT mcpm.hospitalno AS case_no, mcpm.mhiscode, mcpm.caseid, 1 AS tagtype, mdrugm.startdatetime, mcpm.patientname, phsrhosp.drug_unique_code AS drugcode, phsrhosp.drugname, mdrugm.doctorcode, mdrugm.deptcode, phsrhosp.modulename, phsrhosp.warning, CASE WHEN mddrug.antilevel = 1 THEN '非限制使用' WHEN mddrug.antilevel = 2 THEN '限制使用' WHEN mddrug.antilevel = 3 THEN '特殊使用' ELSE '其它' END AS antilevel, mddrug.classid, mddrug.jdmtype, mddrug.is_anti, mddrug.antitype, mdrugm.is_allow ";
		sql += " 	,mhmr.deptmatch_scheme,mhmr.doctormatch_scheme,mhmr.drugmatch_scheme ";
		sql += " 	FROM pharm_screenresults AS phsrhosp ";
		sql += " 	INNER JOIN mc_inhosp_drugorder_main AS mdrugm ON phsrhosp.caseid = mdrugm.caseid AND phsrhosp.mhiscode = mdrugm.mhiscode AND phsrhosp.cid = mdrugm.cid ";
		sql += " 	INNER JOIN mc_inhosp_patient_medinfo AS mcpm ON phsrhosp.caseid = mcpm.caseid 	AND phsrhosp.mhiscode = mcpm.mhiscode ";
		sql += " 	INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mcpm.mhiscode ";
		sql += " 	LEFT JOIN mc_dict_drug AS mddrug ON phsrhosp.drug_unique_code = mddrug.drugcode 	AND mhmr.drugmatch_scheme = mddrug.match_scheme ";
		// sql += " WHERE phsrhosp.moduleid IN(118, 119, 120, 172) AND phsrhosp.patstatus = 1 ";
		sql += " )mdrugm ";
		sql += " INNER JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mdrugm.mhiscode ";
		sql += " LEFT JOIN mc_dict_doctor mddoctor ON mdrugm.doctorcode = mddoctor.doctorcode AND mddoctor.match_scheme = mhmr.doctormatch_scheme ";
		sql += " LEFT JOIN mc_dict_dept mddept ON mdrugm.deptcode = mddept.deptcode AND mddept.match_scheme = mhmr.deptmatch_scheme ";
		sql += " ORDER BY mdrugm.mhiscode, mdrugm.startdatetime ";

		sql += " ) tmp2 ";
		sql += " on tmp1.caseid=tmp2.caseid and tmp1.mhiscode = tmp2.mhiscode ";
		sql += " set  tmp1.case_no = tmp2.case_no, tmp1.patientname = tmp2.patientname, tmp1.deptname= tmp2.deptname, tmp1.doctorname= tmp2.doctorname, tmp1.ilevel= tmp2.ilevel, tmp1.classid= tmp2.classid, tmp1.jdmtype= tmp2.jdmtype, tmp1.is_anti= tmp2.is_anti, tmp1.antitype= tmp2.antitype, tmp1.antilevel= tmp2.antilevel, tmp1.startdatetime= tmp2.startdatetime, tmp1.is_allow= tmp2.is_allow ";
		sql += ",tmp1.deptmatch_scheme=tmp2.deptmatch_scheme,tmp1.doctormatch_scheme=tmp2.doctormatch_scheme,tmp1.drugmatch_scheme=tmp2.drugmatch_scheme";

		sql += " where tmp1.case_no is null or tmp1.case_no='' ";

		getJdbcTemplate().update(sql);

		logger.info("[越权冗余处理] 1)在院（pharm_screenresults_hosp）审查结果表->结束");
	}

	/**
	 * <ul>
	 * <li>方法名：  tmp_basedrug_report </li>
	 * <li>功能描述：7.3  基本药物使用情况统计表(tmp_basedrug_report) </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月27日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void tmp_basedrug_report(int startdate, int enddate) {
		logger.info("7.3  基本药物使用情况统计表(tmp_basedrug_report)->开始");
		String sql = "";
		try {
			// 清洗数据
			for (int i = 1; i <= 12; i++) {
				// 先根据日期删除数据
				sql = String.format("delete from tmp_basedrug_report_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				// 删除在院数据
				sql = String.format("delete from tmp_basedrug_report_%02d where patstatus=1 ", i);
				getJdbcTemplate().update(sql);
			}
			sql = "delete from tmp_basedrug_report where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			// 删除在院数据
			sql = "delete from tmp_basedrug_report where patstatus=1 ";
			getJdbcTemplate().update(sql);

			// 门/急诊处理规则
			sql = "";
			sql += " insert into tmp_basedrug_report (caseid,ienddate,mhiscode,deptcode,doctorcode,itemcode,is_basedrug,cost,drugtype,is_use,costtime,patstatus) ";
			sql += " SELECT DISTINCT a.caseid,a.ienddate,a.mhiscode,a.deptcode,a.doctorcode,a.itemcode,mdd.is_basedrug,a.cost,mdd.drugtype,a.is_use,a.costtime, ";
			sql += " CASE WHEN mcp.is_emergency = 1 THEN 3 ELSE 2 END AS patstatus ";
			sql += " FROM mc_clinic_drugcost_costtime AS a ";
			sql += " INNER JOIN mc_clinic_patient_medinfo mcp ON a.caseid = mcp.caseid AND a.mhiscode = mcp.mhiscode ";
			sql += " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS mdd ON mdd.drugcode = a.itemcode AND mhmr.drugmatch_scheme = mdd.match_scheme ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 出院处理规则
			sql = "";
			sql += " insert into tmp_basedrug_report (caseid,ienddate,mhiscode,deptcode,doctorcode,itemcode,is_basedrug,cost,drugtype,is_use,costtime,patstatus) ";
			sql += " SELECT DISTINCT a.caseid,a.ienddate,a.mhiscode,a.deptcode,a.doctorcode,a.itemcode,mdd.is_basedrug,a.cost,mdd.drugtype,a.is_use,a.costtime,0 AS patstatus ";
			sql += " FROM mc_outhosp_drugcost_costtime AS a ";
			sql += " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS mdd ON mdd.drugcode = a.itemcode AND mhmr.drugmatch_scheme = mdd.match_scheme ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 在院处理规则
			sql = "";
			sql += " insert into tmp_basedrug_report (caseid,mhiscode,deptcode,doctorcode,itemcode,is_basedrug,cost,drugtype,is_use,costtime,patstatus) ";
			sql += " SELECT DISTINCT a.caseid,a.mhiscode,a.deptcode,a.doctorcode,a.itemcode,mdd.is_basedrug,a.cost,mdd.drugtype,a.is_use,a.costtime,1 AS patstatus ";
			sql += " FROM mc_inhosp_drugcost_costtime AS a ";
			sql += " INNER JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_dict_drug AS mdd ON mdd.drugcode = a.itemcode AND mhmr.drugmatch_scheme = mdd.match_scheme ";
			sql += " WHERE a.is_use = 1 ";
			getJdbcTemplate().update(sql);

			logger.info("7.3  基本药物使用情况统计表(tmp_basedrug_report)->结束");
		} catch (Exception e) {
			logger.error("7.3  基本药物使用情况统计表(tmp_basedrug_report)->执行错误：" + e.getMessage(), e);
		}
	}

}
