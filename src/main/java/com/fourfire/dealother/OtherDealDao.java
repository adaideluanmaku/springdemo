package com.fourfire.dealother;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.medicom.modules.persistence.JdbcGenericMysqlDao;

/**
 * 
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  OtherDealDao </li>
 * <li>类描述：其他冗余数据处理   </li>
 * <li>创建人：jy </li>
 * <li>创建时间：2017年8月2日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Repository
public class OtherDealDao extends JdbcGenericMysqlDao {

	private final Logger logger = LoggerFactory.getLogger(OtherDealDao.class);

	public void deal() {
		try {
			String sql = "";
			sql = "REPLACE into tmp_view_outhosp_antiorder select * from view_outhosp_antiorder";
			int len = getJdbcTemplate().update(sql);
			logger.info("[出院 抗菌药物属性 冗余数据处理]--{}", len);

			sql = "REPLACE into tmp_tmp_view_clinic_druguseway select * from view_clinic_druguseway";
			len = getJdbcTemplate().update(sql);
			logger.info("[门诊 药品途径 冗余数据处理]--{}", len);

			sql = "REPLACE into tmp_view_outhosp_druguseway select * from view_outhosp_druguseway";
			len = getJdbcTemplate().update(sql);
			logger.info("[门诊 药品途径 冗余数据处理]--{}", len);

			sql = "REPLACE into tmp_view_outhosp_pt_operation select * from view_outhosp_pt_operation";
			len = getJdbcTemplate().update(sql);
			logger.info("[出院 手术信息 冗余数据处理]--{}", len);

		} catch (Exception ex) {
			log.error("其他冗余数据处理 ", ex);
		}
	}
}
