package com.fourfire.dealother;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.medicom.modules.persistence.JdbcGenericMysqlDao;

/**
 * 
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  DrugStatService </li>
 * <li>类描述： 药品统计  </li>
 * <li>创建人：jy </li>
 * <li>创建时间：2017年6月23日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Repository
public class DrugStatDao extends JdbcGenericMysqlDao {

	private final Logger logger = LoggerFactory.getLogger(DrugStatDao.class);

	/**药品处理的主方法*/
	public void deal(String startdate, String enddate) {
		try {
			// 清洗数据
			int startdateInt = Integer.parseInt(startdate.replace("-", ""));
			int enddateInt = Integer.parseInt(enddate.replace("-", ""));

			// 6.1 药品使用人次统计表
			tmp_drugcasid(startdateInt, enddateInt);

			// 6.3 药品消耗情况及使用量DDDs医院统计表
			tmp_hosp_ddds_cost(startdateInt, enddateInt);

			// 6.6 药品使用金额及数量医院排名表
			tmp_report_hosp_drug_cost(startdateInt, enddateInt);

			// 6.7 医院药品品种数统计表
			tmp_report_drugnum(startdateInt, enddateInt);

			// 6.8 住院患者静脉输液情况统计表
			tmp_itemnum_cost(startdateInt, enddateInt);

			// 6.2 药品使用金额及数量医院排名表，跟6.6重复，按6.6来，这里去掉。
			// tmp_hosp_drug_cost(startdateInt, enddateInt);
		} catch (Exception ex) {
			logger.error("6.X 药品统计的主方法异常", ex);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  tmp_drugcasid </li>
	 * <li>功能描述：6.1 药品使用人次医院统计表(tmp_drugcasid)</li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月28日 </li>
	 * </ul>
	 */
	private void tmp_drugcasid(int startdate, int enddate) {
		logger.info("药品统计分析数据预处理->6.1 药品使用人次医院统计表(tmp_drugcasid)->开始");
		String sql = "";
		// 6.1.1 按名称+剂型+规格+厂家统计
		try {
			for (int i = 1; i <= 12; i++) {
				// 先根据日期删除数据
				sql = String.format("delete from tmp_drugcasid_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				// 删除在院数据
				sql = String.format("delete from tmp_drugcasid_%02d where patstatus=1 ", i);
				getJdbcTemplate().update(sql);
			}
			
			sql = "delete from tmp_drugcasid where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			
			// 删除在院数据
			sql = "delete from tmp_drugcasid where patstatus=1 ";
			getJdbcTemplate().update(sql);

			// 门急诊处理规则（is_emergency=1急诊，is_emergency=0门诊）
			sql = "";
			sql += " INSERT INTO tmp_drugcasid (ienddate, drugindex, caseid, drugmatch_scheme , mhiscode, costtime, drugtype, is_anti, antitype , antilevel, drugformtype, deptcode , doctorcode, druggroupcode, patstatus, druggroupname)  ";
			sql += " SELECT a.ienddate, a.drugindex, a.caseid, m.drugmatch_scheme , a.mhiscode, a.costtime, mdd.drugtype, mdd.is_anti, mdd.antitype , mdd.antilevel, mdd.drugformtype, a.deptcode , a.doctorcode, mdd.druggroupcode, CASE WHEN b.is_emergency = 1 THEN 3 ELSE 2 END AS patstatus, ";
			sql += " CASE WHEN mdd.druggroupcode = - 1 THEN '未知通用名' ELSE mdd.druggroupname END AS druggroupname ";
			sql += " FROM mc_clinic_drugcost_costtime a ";
			sql += " INNER JOIN mc_clinic_patient_medinfo b ON b.caseid = a.caseid AND a.mhiscode = b.mhiscode ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			sql = "";
			// 在院处理规则
			sql += " INSERT INTO tmp_drugcasid (drugindex, caseid, drugmatch_scheme , mhiscode, costtime, drugtype, is_anti, antitype , antilevel, drugformtype, patstatus, deptcode , doctorcode, druggroupcode, druggroupname)  ";
			sql += " SELECT a.drugindex, a.caseid, m.drugmatch_scheme , a.mhiscode, a.costtime,mdd.drugtype, mdd.is_anti, mdd.antitype , mdd.antilevel, mdd.drugformtype, 1 as patstatus, a.deptcode , a.doctorcode, mdd.druggroupcode, ";
			sql += " CASE WHEN mdd.druggroupcode = - 1 THEN '未知通用名' ELSE mdd.druggroupname END AS druggroupname ";
			sql += " FROM mc_inhosp_drugcost_costtime a ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " WHERE  a.is_use = 1 ";

			getJdbcTemplate().update(sql);

			// 出院处理规则
			sql = "";
			sql += " INSERT INTO tmp_drugcasid (ienddate, drugindex, caseid, drugmatch_scheme , mhiscode, costtime, drugtype, is_anti, antitype , antilevel, drugformtype, patstatus, deptcode , doctorcode, druggroupcode, druggroupname) ";
			sql += " SELECT a.ienddate, a.drugindex, a.caseid, m.drugmatch_scheme , a.mhiscode, a.costtime, mdd.drugtype, mdd.is_anti, mdd.antitype , mdd.antilevel, mdd.drugformtype, 0 AS patstatus, a.deptcode , a.doctorcode, mdd.druggroupcode, ";
			sql += " CASE WHEN mdd.druggroupcode = - 1 THEN '未知通用名' ELSE mdd.druggroupname END AS druggroupname ";
			sql += " FROM mc_outhosp_drugcost_costtime a  ";
			sql += " INNER JOIN mc_hospital_match_relation m   ON m.mhiscode = a.mhiscode  ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode  ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 处理药品相关字段
			sql = "";
			sql += " update tmp_drugcasid b ";
			sql += " LEFT JOIN mc_dict_drugindex a ON b.drugindex = a.drugindex ";
			sql += " set b.drugcode = a.drugcode,b.drugname = a.drugname,b.drugform = a.drugform,b.drugspec = a.drugspec,b.drugsccj = a.drugsccj  ";

			getJdbcTemplate().update(sql);

			// 6.2.2 按通用名称统计
			// 如果druggroupcode字段为空则插入-1,通用名编码
			sql = "update tmp_drugcasid set druggroupcode=-1,druggroupname='未知通用名' where druggroupcode is null or druggroupcode='' ";

			getJdbcTemplate().update(sql);

			logger.info("药品统计分析数据预处理->6.1 药品使用人次医院统计表(tmp_drugcasid)->结束");
		} catch (Exception e) {
			logger.error("药品统计分析数据预处理->6.1 药品使用人次医院统计表(tmp_drugcasid)->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  tmp_hosp_ddds_cost </li>
	 * <li>功能描述：6.3  药品消耗情况及使用量DDDs医院统计表（tmp_hosp_ddds_cost） </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月7日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void tmp_hosp_ddds_cost(int startdate, int enddate) {
		logger.info("药品统计分析数据预处理->6.3  药品消耗情况及使用量DDDs医院统计表 (tmp_hosp_ddds_cost)->开始");
		String sql = "";
		try {
			// 清洗数据
			for (int i = 1; i <= 12; i++) {
				// 先根据日期删除数据
				sql = String.format("delete from tmp_hosp_ddds_cost_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				// 删除在院数据
				sql = String.format("delete from tmp_hosp_ddds_cost_%02d where patstatus=1 ", i);
				getJdbcTemplate().update(sql);
			}
			sql = "delete from tmp_hosp_ddds_cost where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			// 删除在院数据
			sql = "delete from tmp_hosp_ddds_cost where patstatus=1 ";
			getJdbcTemplate().update(sql);

			// 门/急诊处理规则
			sql = "";
			sql += " INSERT INTO tmp_hosp_ddds_cost (ienddate,drugmatch_scheme, drugindex, itemunit, itemnum,cost,costtime, mhiscode, deptcode, route_type, patstatus) ";
			sql += " SELECT mcdc.ienddate,mhmr.drugmatch_scheme, mcdc.drugindex, mcdc.itemunit, mcdc.itemnum, mcdc.cost , mcdc.costtime, mcdc.mhiscode, mcdc.deptcode, mdr.route_type, CASE WHEN mcci.is_emergency = 1 THEN 3 ELSE 2 END AS patstatus ";
			sql += " FROM mc_clinic_drugcost_costtime mcdc ";
			sql += " INNER JOIN mc_clinic_caseid_ienddate mcci ON mcci.caseid = mcdc.caseid AND mcci.mhiscode = mcdc.mhiscode ";
			sql += " LEFT JOIN mc_hospital_match_relation mhmr ON mhmr.mhiscode = mcdc.mhiscode ";
			sql += " LEFT JOIN mc_dict_route mdr ON mdr.routecode = mcdc.routecode AND mdr.match_scheme = mhmr.routematch_scheme ";
			sql += " where mcdc.is_use = 1 and mcdc.ienddate>=? and mcdc.ienddate<?  ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 出院处理规则
			sql = "";
			sql += " INSERT INTO tmp_hosp_ddds_cost (ienddate,drugmatch_scheme, drugindex, itemunit, itemnum,cost,costtime, mhiscode, deptcode, route_type, is_out, patstatus) ";
			sql += " SELECT mcdc.ienddate,mhmr.drugmatch_scheme, mcdc.drugindex, mcdc.itemunit, mcdc.itemnum, mcdc.cost , mcdc.costtime, mcdc.mhiscode, mcdc.deptcode, mdr.route_type, mcdc.is_out,1 AS patstatus ";
			sql += " FROM mc_outhosp_drugcost_costtime mcdc ";
			sql += " LEFT JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mcdc.mhiscode ";
			sql += " LEFT JOIN mc_dict_route AS mdr ON mdr.routecode = mcdc.routecode AND mdr.match_scheme = mhmr.routematch_scheme ";
			sql += " where mcdc.is_use = 1 and mcdc.ienddate>=? and mcdc.ienddate<?  ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 在院处理规则
			sql = "";
			sql += " INSERT INTO tmp_hosp_ddds_cost (drugmatch_scheme, drugindex, itemunit, itemnum,cost,costtime, mhiscode, deptcode, route_type, is_out, patstatus) ";
			sql += " SELECT mhmr.drugmatch_scheme, mcdc.drugindex, mcdc.itemunit, mcdc.itemnum, mcdc.cost , mcdc.costtime, mcdc.mhiscode, mcdc.deptcode, mdr.route_type, mcdc.is_out,1 AS patstatus ";
			sql += " FROM mc_inhosp_drugcost_costtime mcdc ";
			sql += " LEFT JOIN mc_hospital_match_relation AS mhmr ON mhmr.mhiscode = mcdc.mhiscode ";
			sql += " LEFT JOIN mc_dict_route AS mdr ON mdr.routecode = mcdc.routecode AND mdr.match_scheme = mhmr.routematch_scheme ";
			sql += " where mcdc.is_use = 1 ";

			getJdbcTemplate().update(sql);

			logger.info("药品统计分析数据预处理->6.3  药品消耗情况及使用量DDDs医院统计表 (tmp_hosp_ddds_cost)->结束");
		} catch (Exception e) {
			logger.error("药品统计分析数据预处理->6.3  药品消耗情况及使用量DDDs医院统计表 (tmp_hosp_ddds_cost)->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  tmp_report_hosp_drug_cost（tmp_report_hosp_drug_cost） </li>
	 * <li>功能描述：6.6  药品使用金额及数量医院排名表 </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月7日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void tmp_report_hosp_drug_cost(int startdate, int enddate) {
		logger.info("药品统计分析数据预处理->6.6  药品使用金额及数量医院排名表(tmp_report_hosp_drug_cost)->开始");
		String sql = "";
		try {
			// 清洗数据
			for (int i = 1; i <= 12; i++) {
				// 先根据日期删除数据
				sql = String.format("delete from tmp_report_hosp_drug_cost_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				// 删除在院数据
				sql = String.format("delete from tmp_report_hosp_drug_cost_%02d where patstatus=1 ", i);
				getJdbcTemplate().update(sql);
				
				//
				sql = String.format("delete from tmp_report_hosp_drug_cost_%02d where caseid in(select DISTINCT caseid from mc_clinic_drugcost_costtime a  where a.is_use = 1 and a.ienddate>=? and a.ienddate<?) ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				
				sql = String.format("delete from tmp_report_hosp_drug_cost_%02d where caseid in(select DISTINCT caseid from mc_outhosp_drugcost_costtime a where a.is_use = 1 and a.ienddate>=? and a.ienddate<?) ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				
				sql = String.format("delete from tmp_report_hosp_drug_cost_%02d where caseid in(select DISTINCT caseid from mc_inhosp_drugcost_costtime a where a.is_use = 1) ", i);
				getJdbcTemplate().update(sql);
			}
			//没分表 主表也需要处理
			sql = "delete from tmp_report_hosp_drug_cost where caseid in(select DISTINCT caseid from mc_clinic_drugcost_costtime a  where a.is_use = 1 and a.ienddate>=? and a.ienddate<?) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			
			sql = "delete from tmp_report_hosp_drug_cost where caseid in(select DISTINCT caseid from mc_outhosp_drugcost_costtime a where a.is_use = 1 and a.ienddate>=? and a.ienddate<?) ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			
			sql = "delete from tmp_report_hosp_drug_cost where caseid in(select DISTINCT caseid from mc_inhosp_drugcost_costtime a where a.is_use = 1) ";
			getJdbcTemplate().update(sql);

			// 门/急诊处理规则
			sql = "";
			sql += " INSERT INTO tmp_report_hosp_drug_cost (caseid, ienddate,drugindex,mhiscode,drugcode,itemcode,itemunit,itemnum,cost,drugmatch_scheme,costtime,typecode, ";
			sql += " drugtype,is_anti,antitype,antilevel,jdmtype,drugformtype,classid,is_basedrug,unitprice,drugform,drugspec,drugsccj,druggroupcode,druggroupname,patstatus ) ";
			sql += " SELECT DISTINCT a.caseid,a.ienddate,a.drugindex,a.mhiscode,mdd.drugcode,a.itemcode,a.itemunit,a.itemnum,a.cost,m.drugmatch_scheme,a.costtime,c.typecode, ";
			sql += " mdd.drugtype,mdd.is_anti,mdd.antitype,mdd.antilevel,mdd.jdmtype,mdd.drugformtype,mdd.classid,mdd.is_basedrug,d.unitprice,d.drugform,d.drugspec,d.drugsccj,mdd.druggroupcode, ";
			sql += " CASE WHEN mdd.druggroupcode = - 1 THEN '未知通用名' ELSE mdd.druggroupname END AS druggroupname, ";
			sql += " CASE WHEN mcp.is_emergency = 1 THEN 3 ELSE 2 END AS patstatus ";
			sql += " FROM mc_clinic_drugcost_costtime a  ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_clinic_patient_medinfo mcp ON a.caseid = mcp.caseid AND a.mhiscode = mcp.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " LEFT JOIN mc_dict_drugindex d ON a.drugindex = d.drugindex ";
			sql += " LEFT JOIN mc_link_drug_type c ON c.drugcode = mdd.drugcode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 出院处理规则
			sql = "";
			sql += " INSERT INTO tmp_report_hosp_drug_cost (caseid, ienddate,drugindex,mhiscode,drugcode,itemcode,itemunit,itemnum,cost,druggroupcode,drugmatch_scheme,costtime, ";
			sql += " typecode,drugtype,is_anti,antitype,antilevel,jdmtype,drugformtype,classid,is_basedrug,unitprice,drugform,drugspec,drugsccj,patstatus,druggroupname ) ";
			sql += " SELECT DISTINCT a.caseid,a.ienddate,a.drugindex,a.mhiscode,mdd.drugcode,a.itemcode,a.itemunit,a.itemnum,a.cost,mdd.druggroupcode,m.drugmatch_scheme,a.costtime,c.typecode, ";
			sql += " mdd.drugtype,mdd.is_anti,mdd.antitype,mdd.antilevel,mdd.jdmtype,mdd.drugformtype,mdd.classid,mdd.is_basedrug,d.unitprice,d.drugform,d.drugspec,d.drugsccj,1 AS patstatus, ";
			sql += " CASE WHEN mdd.druggroupcode = - 1 THEN '未知通用名' ELSE mdd.druggroupname END AS druggroupname ";
			sql += " FROM mc_outhosp_drugcost_costtime a ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " LEFT JOIN mc_dict_drugindex d ON a.drugindex = d.drugindex ";
			sql += " LEFT JOIN mc_link_drug_type c ON c.drugcode = mdd.drugcode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 在院处理规则
			sql = "";
			sql += " INSERT INTO tmp_report_hosp_drug_cost ( caseid,drugindex,mhiscode,drugcode,itemcode,itemunit,itemnum,cost,druggroupcode,drugmatch_scheme,costtime,typecode, ";
			sql += " drugtype,is_anti,antitype,antilevel,jdmtype,drugformtype,classid,is_basedrug,unitprice,drugform,drugspec,drugsccj,patstatus,druggroupname ) ";
			sql += " SELECT DISTINCT a.caseid,a.drugindex,a.mhiscode,mdd.drugcode,a.itemcode,a.itemunit,a.itemnum,a.cost,mdd.druggroupcode,m.drugmatch_scheme,a.costtime,c.typecode, ";
			sql += " mdd.drugtype,mdd.is_anti,mdd.antitype,mdd.antilevel,mdd.jdmtype,mdd.drugformtype,mdd.classid,mdd.is_basedrug,d.unitprice,d.drugform,d.drugspec,d.drugsccj,0 AS patstatus, ";
			sql += " CASE WHEN mdd.druggroupcode = - 1 THEN '未知通用名' ELSE mdd.druggroupname END AS druggroupname ";
			sql += " FROM mc_inhosp_drugcost_costtime a ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " LEFT JOIN mc_dict_drugindex d ON a.drugindex = d.drugindex ";
			sql += " LEFT JOIN mc_link_drug_type c ON c.drugcode = mdd.drugcode ";
			sql += " where a.is_use = 1 ";
			getJdbcTemplate().update(sql);

			logger.info("药品统计分析数据预处理->6.6  药品使用金额及数量医院排名表(tmp_report_hosp_drug_cost)->结束");
		} catch (Exception e) {
			logger.error("药品统计分析数据预处理->6.6  药品使用金额及数量医院排名表(tmp_report_hosp_drug_cost)->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  tmp_report_drugnum </li>
	 * <li>功能描述：6.7  医院药品品种数统计表（tmp_report_drugnum） </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月7日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void tmp_report_drugnum(int startdate, int enddate) {
		logger.info("药品统计分析数据预处理->6.7  医院药品品种数统计表(tmp_report_drugnum)->开始");
		String sql = "";
		try {
			// 清洗数据
			for (int i = 1; i <= 12; i++) {
				// 先根据日期删除数据
				sql = String.format("delete from tmp_report_drugnum_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				// 删除在院数据
				sql = String.format("delete from tmp_report_drugnum_%02d where patstatus=1 ", i);
				getJdbcTemplate().update(sql);
			}
			
			sql = "delete from tmp_report_drugnum where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			// 删除在院数据
			sql = "delete from tmp_report_drugnum where patstatus=1 ";
			getJdbcTemplate().update(sql);

			// 门/急诊处理规则
			sql = "";
			sql += " INSERT INTO tmp_report_drugnum ( ienddate,mhiscode,itemcode,druggroupcode,routecode,deptcode,doctorcode,deptmatch_scheme,doctormatch_scheme,costtime, ";
			sql += " is_anti,antitype,antilevel,route_type,classid,drugformtype,is_basedrug,is_basedrug_p,drugtype,is_sugarmed,is_bloodmed,is_srpreparations,patstatus,deptname,doctorname ) ";
			sql += " SELECT DISTINCT a.ienddate,a.mhiscode,a.itemcode,mdd.druggroupcode,a.routecode,a.deptcode,a.doctorcode,m.deptmatch_scheme,m.doctormatch_scheme,a.costtime, ";
			sql += " mdd.is_anti,mdd.antitype,mdd.antilevel,mdr.route_type,mdd.classid,mdd.drugformtype,mdd.is_basedrug,mdd.is_basedrug_p,mdd.drugtype,mdd.is_sugarmed,mdd.is_bloodmed,mdd.is_srpreparations, ";
			sql += " CASE WHEN mcp.is_emergency = 1 THEN 3 ELSE 2 END AS patstatus, ";
			sql += " CASE WHEN ( mdept.deptname = '' OR mdept.deptname IS NULL ) THEN '未知科室' ELSE mdept.deptname END AS deptname, ";
			sql += " CASE WHEN ( mdct.doctorname = '' OR mdct.doctorname IS NULL ) THEN '未知医生' ELSE mdct.doctorname END AS doctorname ";
			sql += " FROM mc_clinic_drugcost_costtime a ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " INNER JOIN mc_clinic_patient_medinfo mcp ON a.caseid = mcp.caseid AND a.mhiscode = mcp.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " LEFT JOIN mc_dict_dept mdept ON m.deptmatch_scheme = mdept.match_scheme AND mdept.deptcode = a.deptcode ";
			sql += " LEFT JOIN mc_dict_doctor mdct ON m.doctormatch_scheme = mdct.match_scheme AND mdct.doctorcode = a.doctorcode ";
			sql += " LEFT JOIN mc_dict_route mdr ON m.routematch_scheme = mdr.match_scheme AND mdr.routecode = a.routecode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 出院处理规则
			sql = "";
			sql += " INSERT INTO tmp_report_drugnum ( ienddate,mhiscode,itemcode,druggroupcode,routecode,deptcode,doctorcode,deptmatch_scheme,doctormatch_scheme,costtime, ";
			sql += " is_anti,antitype,antilevel,route_type,classid,drugformtype,is_basedrug,is_basedrug_p,drugtype,is_sugarmed,is_bloodmed,is_srpreparations,patstatus,deptname,doctorname ) ";
			sql += " SELECT DISTINCT a.ienddate,a.mhiscode,a.itemcode,mdd.druggroupcode,a.routecode,a.deptcode,a.doctorcode,m.deptmatch_scheme,m.doctormatch_scheme,a.costtime, ";
			sql += " mdd.is_anti,mdd.antitype,mdd.antilevel,mdr.route_type,mdd.classid,mdd.drugformtype,mdd.is_basedrug,mdd.is_basedrug_p,mdd.drugtype,mdd.is_sugarmed,mdd.is_bloodmed,mdd.is_srpreparations,1 AS patstatus, ";
			sql += " CASE WHEN ( mdept.deptname = '' OR mdept.deptname IS NULL ) THEN '未知科室' ELSE mdept.deptname END AS deptname, ";
			sql += " CASE WHEN ( mdct.doctorname = '' OR mdct.doctorname IS NULL ) THEN '未知医生' ELSE mdct.doctorname END AS doctorname ";
			sql += " FROM mc_outhosp_drugcost_costtime a ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " LEFT JOIN mc_dict_dept mdept ON m.deptmatch_scheme = mdept.match_scheme AND mdept.deptcode = a.deptcode ";
			sql += " LEFT JOIN mc_dict_doctor mdct ON m.doctormatch_scheme = mdct.match_scheme AND mdct.doctorcode = a.doctorcode ";
			sql += " LEFT JOIN mc_dict_route mdr ON m.routematch_scheme = mdr.match_scheme AND mdr.routecode = a.routecode ";
			sql += " WHERE a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";

			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 在院处理规则
			sql = "";
			sql += " INSERT INTO tmp_report_drugnum ( mhiscode,itemcode,druggroupcode,routecode,deptcode,doctorcode,deptmatch_scheme,doctormatch_scheme,costtime, ";
			sql += " is_anti,antitype,antilevel,route_type,classid,drugformtype,is_basedrug,is_basedrug_p,drugtype,is_sugarmed,is_bloodmed,is_srpreparations,patstatus,deptname,doctorname ) ";
			sql += " SELECT DISTINCT a.mhiscode,a.itemcode,mdd.druggroupcode,a.routecode,a.deptcode,a.doctorcode,m.deptmatch_scheme,m.doctormatch_scheme,a.costtime, ";
			sql += " mdd.is_anti,mdd.antitype,mdd.antilevel,mdr.route_type,mdd.classid,mdd.drugformtype,mdd.is_basedrug,mdd.is_basedrug_p,mdd.drugtype,mdd.is_sugarmed,mdd.is_bloodmed,mdd.is_srpreparations,1 AS patstatus, ";
			sql += " CASE WHEN ( mdept.deptname = '' OR mdept.deptname IS NULL ) THEN '未知科室' ELSE mdept.deptname END AS deptname, ";
			sql += " CASE WHEN ( mdct.doctorname = '' OR mdct.doctorname IS NULL ) THEN '未知医生' ELSE mdct.doctorname END AS doctorname ";
			sql += " FROM mc_inhosp_drugcost_costtime a ";
			sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
			sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND mdd.drugcode = a.itemcode ";
			sql += " LEFT JOIN mc_dict_dept mdept ON m.deptmatch_scheme = mdept.match_scheme AND mdept.deptcode = a.deptcode ";
			sql += " LEFT JOIN mc_dict_doctor mdct ON m.doctormatch_scheme = mdct.match_scheme AND mdct.doctorcode = a.doctorcode ";
			sql += " LEFT JOIN mc_dict_route mdr ON m.routematch_scheme = mdr.match_scheme AND mdr.routecode = a.routecode ";
			sql += " WHERE a.is_use = 1 ";

			getJdbcTemplate().update(sql);

			logger.info("药品统计分析数据预处理->6.7  医院药品品种数统计表(tmp_report_drugnum)->结束");
		} catch (Exception e) {
			logger.error("药品统计分析数据预处理->6.7  医院药品品种数统计表(tmp_report_drugnum)->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  tmp_report_drugnum </li>
	 * <li>功能描述：6.8 住院患者静脉输液情况统计表（tmp_itemnum_cost） </li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月17日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void tmp_itemnum_cost(int startdate, int enddate) {
		logger.info("药品统计分析数据预处理->6.8  医院药品品种数统计表(tmp_itemnum_cost)->开始");
		String sql = "";
		try {
			// 清洗数据
			for (int i = 1; i <= 12; i++) {
				// 先根据日期删除数据
				sql = String.format("delete from tmp_itemnum_cost_%02d where ienddate>=? and ienddate<? ", i);
				getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
				// 删除在院数据
				sql = String.format("delete from tmp_itemnum_cost_%02d where patstatus=1 ", i);
				getJdbcTemplate().update(sql);
			}
			sql = "delete from tmp_itemnum_cost where ienddate>=? and ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
			// 删除在院数据
			sql = "delete from tmp_itemnum_cost where patstatus=1 ";
			getJdbcTemplate().update(sql);

			// 出院处理规则
			sql = "";
			sql += " insert into tmp_itemnum_cost ( drugindex,itemunit,itemnum,cost,mhiscode,is_out,routecode,costtime,deptcode,ienddate,patstatus  ) ";
			sql += " SELECT a.drugindex,a.itemunit,a.itemnum,a.cost,a.mhiscode,a.is_out,a.routecode,a.costtime,a.deptcode,a.ienddate,0 ";
			sql += " FROM mc_outhosp_drugcost_costtime a WHERE a.ienddate>=? and a.ienddate<? ";
			getJdbcTemplate().update(sql, new Object[] { startdate, enddate });

			// 在院处理规则
			sql = "";
			sql += " insert into tmp_itemnum_cost ( drugindex,itemunit,itemnum,cost,mhiscode,is_out,routecode,costtime,deptcode,patstatus ) ";
			sql += " SELECT b.drugindex,b.itemunit,b.itemnum,b.cost,b.mhiscode,b.is_out,b.routecode,b.costtime,b.deptcode,1 ";
			sql += " FROM mc_inhosp_drugcost_costtime b ";
			getJdbcTemplate().update(sql);

			logger.info("药品统计分析数据预处理->6.8  医院药品品种数统计表(tmp_itemnum_cost)->结束");
		} catch (Exception e) {
			logger.error("药品统计分析数据预处理->6.8  医院药品品种数统计表(tmp_itemnum_cost)->执行错误：" + e.getMessage(), e);
		}
	}

	/**
	 * <ul>
	 * <li>方法名：  tmp_hosp_drug_cost </li>
	 * <li>功能描述：6.2 药品使用金额及数量医院排名表 (tmp_hosp_drug_cost)，跟6.6重复，按6.6来。</li>
	 * <li>创建人：  maYJ </li>
	 * <li>创建时间：2017年7月6日 </li>
	 * </ul> 
	 * @param startdate
	 * @param enddate
	 */
	private void tmp_hosp_drug_cost(int startdate, int enddate) {
		// logger.info("药品统计分析数据预处理->6.2 药品使用金额及数量医院排名表 (tmp_hosp_drug_cost)->开始");
		// String sql = "";
		// try {
		// //先根据日期删除数据
		// sql = "delete from tmp_hosp_drug_cost where ienddate>=? and ienddate<? ";
		// getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
		// //删除在院数据
		// sql = "";
		// sql = "delete from tmp_hosp_drug_cost where patstatus=1 ";
		// getJdbcTemplate().update(sql);
		//
		// // 门急诊处理规则（is_emergency=1急诊，is_emergency=0门诊）
		// sql = "";
		// sql += " INSERT INTO tmp_hosp_drug_cost
		// (ienddate,drugindex,itemunit,itemnum,cost,mhiscode,itemcode,drugmatch_scheme,costtime,typecode,classid,drugtype,drugformtype,
		// ";
		// sql += "
		// is_anti,antitype,antilevel,jdmtype,is_basedrug,unitprice,drugcode,drugname,drugform,drugspec,drugsccj,patstatus,druggroupname,druggroupcode)
		// ";
		// sql += " SELECT
		// a.ienddate,a.drugindex,a.itemunit,a.itemnum,a.cost,a.mhiscode,a.itemcode,m.drugmatch_scheme,a.costtime,c.typecode,mdd.classid,mdd.drugtype,mdd.drugformtype,
		// ";
		// sql += " mdd.is_anti, mdd.antitype, mdd.antilevel ,
		// mdd.jdmtype,mdd.is_basedrug,d.unitprice,d.drugcode,d.drugname,d.drugform,d.drugspec,d.drugsccj,
		// ";
		// sql += " CASE WHEN mcp.is_emergency = 1 THEN 3 WHEN mcp.is_emergency = 0 THEN 2 ELSE
		// mcp.is_emergency END AS patstatus, ";
		// sql += " CASE WHEN (mdd.druggroupname IS NULL OR mdd.druggroupname = '') THEN '未知通用名称'
		// ELSE mdd.druggroupname END as druggroupname, ";
		// sql += " CASE WHEN (mdd.druggroupcode IS NULL OR mdd.druggroupcode = '') THEN '-1' ELSE
		// mdd.druggroupcode END as druggroupcode ";
		// sql += " FROM mc_clinic_drugcost_costtime a ";
		// sql += " INNER JOIN mc_clinic_patient_medinfo mcp ON a.caseid = mcp.caseid AND a.mhiscode
		// = mcp.mhiscode ";
		// sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
		// sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND
		// mdd.drugcode = a.itemcode ";
		// sql += " LEFT JOIN mc_dict_drugindex d ON a.drugindex = d.drugindex ";
		// sql += " LEFT JOIN mc_link_drug_type c ON c.drugcode = mdd.drugcode ";
		// sql += " where a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";
		//
		// getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
		//
		// // 出院处理规则
		// sql = "";
		// sql += " INSERT INTO tmp_hosp_drug_cost
		// (ienddate,drugindex,itemunit,itemnum,cost,mhiscode,itemcode,drugmatch_scheme,costtime,typecode,classid,drugtype,drugformtype,
		// ";
		// sql += "
		// is_anti,antitype,antilevel,jdmtype,is_basedrug,unitprice,drugcode,drugname,drugform,drugspec,drugsccj,patstatus,druggroupname,druggroupcode)
		// ";
		// sql += " SELECT
		// a.ienddate,a.drugindex,a.itemunit,a.itemnum,a.cost,a.mhiscode,a.itemcode,m.drugmatch_scheme,a.costtime,c.typecode,mdd.classid,mdd.drugtype,mdd.drugformtype,
		// ";
		// sql += " mdd.is_anti, mdd.antitype, mdd.antilevel ,
		// mdd.jdmtype,mdd.is_basedrug,d.unitprice,d.drugcode,d.drugname,d.drugform,d.drugspec,d.drugsccj,0
		// AS patstatus, ";
		// sql += " CASE WHEN (mdd.druggroupname IS NULL OR mdd.druggroupname = '') THEN '未知通用名称'
		// ELSE mdd.druggroupname END as druggroupname, ";
		// sql += " CASE WHEN (mdd.druggroupcode IS NULL OR mdd.druggroupcode = '') THEN '-1' ELSE
		// mdd.druggroupcode END as druggroupcode ";
		// sql += " FROM mc_outhosp_drugcost_costtime a ";
		// sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
		// sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND
		// mdd.drugcode = a.itemcode ";
		// sql += " LEFT JOIN mc_dict_drugindex d ON a.drugindex = d.drugindex ";
		// sql += " LEFT JOIN mc_link_drug_type c ON c.drugcode = mdd.drugcode ";
		// sql += " where a.is_use = 1 and a.ienddate>=? and a.ienddate<? ";
		//
		// getJdbcTemplate().update(sql, new Object[] { startdate, enddate });
		//
		// // 在院处理规则
		// sql = "";
		// sql += " INSERT INTO tmp_hosp_drug_cost
		// (drugindex,itemunit,itemnum,cost,mhiscode,itemcode,drugmatch_scheme,costtime,typecode,classid,drugtype,drugformtype,
		// ";
		// sql += "
		// is_anti,antitype,antilevel,jdmtype,is_basedrug,unitprice,drugcode,drugname,drugform,drugspec,drugsccj,patstatus,druggroupname,druggroupcode)
		// ";
		// sql += " SELECT
		// a.drugindex,a.itemunit,a.itemnum,a.cost,a.mhiscode,a.itemcode,m.drugmatch_scheme,a.costtime,c.typecode,mdd.classid,mdd.drugtype,mdd.drugformtype,
		// ";
		// sql += " mdd.is_anti, mdd.antitype, mdd.antilevel ,
		// mdd.jdmtype,mdd.is_basedrug,d.unitprice,d.drugcode,d.drugname,d.drugform,d.drugspec,d.drugsccj,1
		// AS patstatus, ";
		// sql += " CASE WHEN (mdd.druggroupname IS NULL OR mdd.druggroupname = '') THEN '未知通用名称'
		// ELSE mdd.druggroupname END as druggroupname, ";
		// sql += " CASE WHEN (mdd.druggroupcode IS NULL OR mdd.druggroupcode = '') THEN '-1' ELSE
		// mdd.druggroupcode END as druggroupcode ";
		// sql += " FROM mc_inhosp_drugcost_costtime a ";
		// sql += " INNER JOIN mc_hospital_match_relation m ON m.mhiscode = a.mhiscode ";
		// sql += " LEFT JOIN mc_dict_drug mdd ON m.drugmatch_scheme = mdd.match_scheme AND
		// mdd.drugcode = a.itemcode ";
		// sql += " LEFT JOIN mc_dict_drugindex d ON a.drugindex = d.drugindex ";
		// sql += " LEFT JOIN mc_link_drug_type c ON c.drugcode = mdd.drugcode ";
		// sql += " where a.is_use = 1 ";
		// getJdbcTemplate().update(sql);
		//
		// //处理药品相关字段
		// sql = "";
		// sql += " update tmp_hosp_drug_cost b ";
		// sql += " LEFT JOIN mc_dict_drugindex a ON b.drugindex = a.drugindex ";
		// sql += " set b.drugcode = a.drugcode, b.drugname = a.drugname, b.drugform = a.drugform,
		// b.drugspec = a.drugspec, b.drugsccj = a.drugsccj ";
		// getJdbcTemplate().update(sql);
		//
		// logger.info("药品统计分析数据预处理->6.2 药品使用金额及数量医院排名表 (tmp_hosp_drug_cost)->结束");
		//
		// } catch (Exception e) {
		// logger.error("药品统计分析数据预处理->6.2 药品使用金额及数量医院排名表
		// (tmp_hosp_drug_cost)->执行错误："+e.getMessage(),e);
		// }
	}

}
