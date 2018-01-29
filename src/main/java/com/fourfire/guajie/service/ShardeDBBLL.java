package com.fourfire.guajie.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fourfire.guajie.dao.ShardDB;
import com.fourfire.guajie.dao.ShardeDBDao;
import com.google.common.collect.Maps;

/**
 * 
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  ShardeDBService </li>
 * <li>类描述： 表到分表  </li>
 * <li>创建人：jy </li>
 * <li>创建时间：2017年6月7日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Service
public class ShardeDBBLL {

	private static Logger log = LoggerFactory.getLogger(ShardeDBBLL.class);

	@Autowired
	private ShardeDBDao shardeDBDao;

	/**日期类型*/
	public enum DATATYPE {
		YYYY_MM, YYYYMM;
	}

	public static Map<String, Integer> ShardDBMap = Maps.newHashMap();

	private static final List<ShardDB> ShardDBList = new ArrayList<ShardDB>() {
		private static final long serialVersionUID = 1L;

		{
			/**
			add(new ShardDB("mc_clinic_allergen"));
			add(new ShardDB("mc_clinic_caseid_ienddate"));
			add(new ShardDB("mc_clinic_cost"));
			add(new ShardDB("mc_clinic_disease"));
			add(new ShardDB("mc_clinic_drugcost_caseid"));
			add(new ShardDB("mc_clinic_drugcost_costtime"));
			add(new ShardDB("mc_clinic_drugorder_detail"));
			add(new ShardDB("mc_clinic_drugorder_main"));
			add(new ShardDB("mc_clinic_exam"));
			add(new ShardDB("mc_clinic_lab"));
			add(new ShardDB("mc_clinic_operation"));
			add(new ShardDB("mc_clinic_order"));
			add(new ShardDB("mc_clinic_patient_baseinfo"));
			add(new ShardDB("mc_clinic_patient_medinfo"));
			add(new ShardDB("mc_clinic_prescription"));
			// 出院
			add(new ShardDB("mc_outhosp_allergen"));
			add(new ShardDB("mc_outhosp_caseid_ienddate"));
			add(new ShardDB("mc_outhosp_cost"));
			add(new ShardDB("mc_outhosp_disease"));
			add(new ShardDB("mc_outhosp_drugcost_caseid"));
			add(new ShardDB("mc_outhosp_drugcost_costtime"));
			add(new ShardDB("mc_outhosp_drugcostdistinct"));
			add(new ShardDB("mc_outhosp_drugorder_detail"));
			add(new ShardDB("mc_outhosp_drugorder_main"));
			add(new ShardDB("mc_outhosp_exam"));
			add(new ShardDB("mc_outhosp_lab"));
			add(new ShardDB("mc_outhosp_operation"));
			add(new ShardDB("mc_outhosp_order"));
			add(new ShardDB("mc_outhosp_patient_baseinfo"));
			add(new ShardDB("mc_outhosp_patient_medinfo"));
			add(new ShardDB("mc_outhosp_temperature"));
			
			//审查 挂接信息
			addAll(ShardDBSingleList);
			*/

			// 挂接信息
//			add(new ShardDB("mc_review_detail"));
//			add(new ShardDB("mc_review_main", "id"));
//			add(new ShardDB("mc_review_question_drugs"));
//
//			// 审查信息
//			add(new ShardDB("mc_screen_info", null, "executetime", "executetime", DATATYPE.YYYYMM));// executetime
			add(new ShardDB("pharm_screenresults", "chkresid"));

//			// 冗余字段
//			// 药品 6.x
//			add(new ShardDB("tmp_drugcasid", null, "costtime", "ienddate", DATATYPE.YYYY_MM));
//			add(new ShardDB("tmp_hosp_ddds_cost", null, "costtime", "ienddate", DATATYPE.YYYY_MM));
//			add(new ShardDB("tmp_report_hosp_drug_cost", null, "costtime", "ienddate", DATATYPE.YYYY_MM));
//			add(new ShardDB("tmp_report_drugnum", null, "costtime", "ienddate", DATATYPE.YYYY_MM));
//			add(new ShardDB("tmp_itemnum_cost", null, "costtime", "ienddate", DATATYPE.YYYY_MM));
//
//			// 合理用药指标统计数据预处理 5.x
//			// 5.1 门诊处方指标
//			add(new ShardDB("tmp_indtmp_clinicpres"));
//			add(new ShardDB("tmp_indtmp_cost"));
//			add(new ShardDB("tmp_indtmp_num"));
//
//			// 5.2 门诊病人指标
//			add(new ShardDB("tmp_indtmp_clinicpt"));
//			add(new ShardDB("tmp_indtmp_costpt"));
//			add(new ShardDB("tmp_indtmp_numpt"));
//
//			// 7.x 其他统计
//			// 7.3 基本药物使用情况统计表
//			add(new ShardDB("tmp_basedrug_report", null, "costtime", "ienddate", DATATYPE.YYYY_MM));

		}
	};

	/**
	 * 单个审查信息
	 */
	private static final List<ShardDB> ShardDBSingleList = new ArrayList<ShardDB>() {
		private static final long serialVersionUID = 1L;

		{
			// 挂接信息
			add(new ShardDB("mc_review_detail"));
			add(new ShardDB("mc_review_main", "id"));
			add(new ShardDB("mc_review_question_drugs"));

			// 审查信息
			add(new ShardDB("mc_screen_info", null, "executetime", "executetime", DATATYPE.YYYYMM));// executetime
			add(new ShardDB("pharm_screenresults", "chkresid"));
		}
	};

	/**
	 * 
	 * <ul>
	 * <li>方法名：  etlCopyToShardDB </li>
	 * <li>功能描述：etl数据处理后,把数据导入月表 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年7月18日 </li>
	 * </ul> 
	 * @param isTest 是否是测试。true:保留正式表;false:删除正式表
	 */
	public synchronized void etlCopyToShardDB(boolean isTest) {

		for (ShardDB fromTB : ShardDBList) {
			// 先删除,插入的废数据
			if (StringUtils.isNotBlank(fromTB.getIdentityKey())) {
				Integer idval = ShardDBMap.get(fromTB.getTablename());
				if (null != idval && idval.intValue() > 1) {
					shardeDBDao.delete(fromTB.getTablename(), fromTB.getIdentityKey(), idval);
				}
			}
			//
			copyAndCleanShardDB(fromTB.getTablename(), fromTB.getShardCol(), fromTB.getDelCol(), fromTB.getShardColType(), isTest);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  screenSingleCopyToShardDB </li>
	 * <li>功能描述：pa单个审查的时候,审查结果也需要插入月表 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年7月25日 </li>
	 * </ul> 
	 * @param isTest
	 */
	public synchronized void screenSingleCopyToShardDB(boolean isTest) {
		// 这里并没有删除废数据了,注意
		for (ShardDB fromTB : ShardDBSingleList) {
			copyAndCleanShardDB(fromTB.getTablename(), fromTB.getShardCol(), fromTB.getDelCol(), fromTB.getShardColType(), isTest);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  copyAndCleanShardDB </li>
	 * <li>功能描述： 按照月份拷贝全数据,并清洗分表</li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月7日 </li>
	 * </ul> 
	 * @param intoTB
	 * @param fromTB
	 */
	private void copyAndCleanShardDB(String fromTB, String shardCol, String delCol, DATATYPE timetype, boolean isTest) {
		try {
			log.info("copyShardeDB 开始处理分表信息 {} {} {}", fromTB, shardCol, delCol);
			String[] delArr = shardeDBDao.getDelCol(delCol, fromTB, timetype);
			List<String> mouths = shardeDBDao.getMouth(shardCol, fromTB, timetype);
			
			String delCoss = "";
			if (!StringUtils.equals(shardCol, delCol)) {// -------1)
				for (int i = 1; i <= 12; i++) {
					String mouth = String.format("%02d", i);
					for (String delCos : delArr) {
						if (delCos.length() == 8) {
							delCoss += delCos + ";";
							shardeDBDao.deleteShardeDBByDelCol(fromTB, delCol, mouth, Integer.parseInt(delCos));
						}
					}
					log.info("{}. copyShardeDB 删除{} {}月  具体日期 {}", i, fromTB, mouth, delCoss);
					delCoss = "";
				}
			} else {
				if (mouths != null && mouths.size() > 0) {
					for (String mouth : mouths) {
						// 清除月表
						if (StringUtils.equals(shardCol, delCol)) {// -------2)
							for (String delCos : delArr) {
								if (delCos.length() == 8 && StringUtils.equals(delCos.substring(4, 6), mouth)) {
									delCoss += delCos + ";";
									shardeDBDao.deleteShardeDBByDelCol(fromTB, delCol, mouth, Integer.parseInt(delCos));
								}
							}
							log.info("copyShardeDB 删除{} {}月 具体日期 {}", fromTB, mouth, delCoss);
							delCoss = "";
						}
					}
				}
			}
			
			if (mouths != null && mouths.size() > 0) {
				for (String mouth : mouths) {
					int len = 0;
					switch (timetype) {
					case YYYYMM:
						len = shardeDBDao.copyShardeDB_YYYYMM(fromTB, shardCol, mouth);
						break;
					case YYYY_MM:
						len = shardeDBDao.copyShardeDB_YYYY_MM(fromTB, shardCol, mouth);
						break;
					}
			
					log.info("copyShardeDB 已处理分表信息 {} {} mouths->{} deal->{}", fromTB, shardCol, mouth, len);
				}
			}
			if (!isTest) {// 测试不删除临时表
				// 用del.不删除自增关系
				shardeDBDao.deleteFromTb(fromTB);// truncateTb(fromTB);
			}
		} catch (Exception ex) {
			log.error("copyShardeDB 异常 ", ex);
			log.error("copyShardeDB 异常{} {} ", fromTB, shardCol);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  copyShardeDB </li>
	 * <li>功能描述： 按照月份拷贝全数据</li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月7日 </li>
	 * </ul> 
	 * @param intoTB
	 * @param fromTB
	 */
	public void copyShardeDB(String fromTB, String shardCol, DATATYPE timetype) {
		try {
			// 由于是 定时处理,所以不存在多大数据问题,就一个事务
			log.info("copyShardeDB 开始处理分表信息 {} {} ", fromTB, shardCol);
			List<String> mouths = shardeDBDao.getMouth(shardCol, fromTB, timetype);
			if (mouths != null && mouths.size() > 0) {
				for (String mouth : mouths) {
					int len = 0;
					switch (timetype) {
					case YYYYMM:
						len = shardeDBDao.copyShardeDB_YYYYMM(fromTB, shardCol, mouth);
						break;
					case YYYY_MM:
						len = shardeDBDao.copyShardeDB_YYYY_MM(fromTB, shardCol, mouth);
						break;
					}

					log.info("copyShardeDB 已处理分表信息 {} {} mouths->{} deal->{}", fromTB, shardCol, mouth, len);
				}
			}
			// 用del.不删除自增关系
			shardeDBDao.deleteFromTb(fromTB);// truncateTb(fromTB);
		} catch (Exception ex) {
			log.error("copyShardeDB 异常 ", ex);
			log.error("copyShardeDB 异常{} {} ", fromTB, shardCol);
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  clearData </li>
	 * <li>功能描述：清洗数据,不要批量处理 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月9日 </li>
	 * </ul> 
	 * @param index
	 */
	public synchronized void clearData(int index) {
		String[] sqlArr = {
				" delete from sa_screenresults where logid not IN (select * FROM(select max(logid) from sa_screenresults GROUP BY caseid)t)",
				"delete from sa_pat_info where logid not in (select logid from sa_screenresults)",
				"delete from sa_pat_orders where logid not in (select logid from sa_screenresults)",
				"delete from sa_pat_operation where logid not in (select logid from sa_screenresults)",
				"delete from sa_pat_allergens where logid not in (select logid from sa_screenresults)",
				"delete from sa_pat_disease where logid not in (select logid from sa_screenresults)" };
		try {
			index = index > (sqlArr.length - 1) ? (sqlArr.length - 1) : index;
			shardeDBDao.getJdbcTemplate().execute(sqlArr[index]);
		} catch (Exception ex) {
			log.error("clearData 异常 ", ex);
			log.error("clearData 异常{} {} ", index, ex.getMessage());
		}
	}

	/**存在自增主键的表,根据月表获取最大id<br>插入一个临时表,并记录在内存中*/
	public synchronized void etlShardDB_init() {
		for (ShardDB fromTB : ShardDBList) {
			if (StringUtils.isNotBlank(fromTB.getIdentityKey())) {
				try {
					// 清除表
					shardeDBDao.getJdbcTemplate().execute("truncate table " + fromTB.getTablename());
					shardeDBDao.getJdbcTemplate().execute("delete from " + fromTB.getTablename());
					shardeDBDao.getJdbcTemplate().execute("truncate table " + fromTB.getTablename());
					//
					int maxid = shardeDBDao.etlShardDB_init(fromTB.getTablename(), fromTB.getIdentityKey());
					ShardDBMap.put(fromTB.getTablename(), maxid);
				} catch (Exception ex) {
					log.error("etlShardDB_init 异常 ", ex);
					log.error("etlShardDB_init 异常{} {} -> {} ", fromTB.getTablename(), fromTB.getIdentityKey(), ex.getMessage());
				}
			}
		}
	}

}
