package com.fourfire.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fourfire.guajie.dao.ShardeDBDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/applicationContext.xml" })
public class 备份表创建 {

	@Autowired(required = true)
	private ShardeDBDao shardeDBDao;

	@Test
	public void c1() {

		final List<String> tb = new ArrayList<String>() {
			{
				add("mc_clinic_operation");
				/**
				add("mc_clinic_allergen");
				add("mc_clinic_caseid_ienddate");
				add("mc_clinic_cost");
				add("mc_clinic_disease");
				add("mc_clinic_drugcost_caseid");
				add("mc_clinic_drugcost_costtime");
				add("mc_clinic_drugorder_detail");
				add("mc_clinic_drugorder_main");
				add("mc_clinic_exam");
				add("mc_clinic_lab");
				add("mc_clinic_operation");
				add("mc_clinic_order");
				add("mc_clinic_patient_baseinfo");
				add("mc_clinic_patient_medinfo");
				add("mc_clinic_prescription");
				add("mc_outhosp_allergen");
				add("mc_outhosp_caseid_ienddate");
				add("mc_outhosp_cost");
				add("mc_outhosp_disease");
				add("mc_outhosp_drugcost_caseid");
				add("mc_outhosp_drugcost_costtime");
				add("mc_outhosp_drugcostdistinct");
				add("mc_outhosp_drugorder_detail");
				add("mc_outhosp_drugorder_main");
				add("mc_outhosp_exam");
				add("mc_outhosp_lab");
				add("mc_outhosp_operation");
				add("mc_outhosp_order");
				add("mc_outhosp_patient_baseinfo");
				add("mc_outhosp_patient_medinfo");
				add("mc_outhosp_temperature");
				add("mc_review_detail");
				add("mc_review_main");
				add("mc_review_question_drugs");
				add("mc_screen_info");
				add("pharm_screenresults");
				add("tmp_drugcasid");
				add("tmp_hosp_ddds_cost");
				add("tmp_report_hosp_drug_cost");
				add("tmp_report_drugnum");
				add("tmp_itemnum_cost");
				add("tmp_indtmp_clinicpres");
				add("tmp_indtmp_cost");
				add("tmp_indtmp_num");
				add("tmp_indtmp_clinicpt");
				add("tmp_indtmp_costpt");
				add("tmp_indtmp_numpt");
				add("tmp_outtmp_hosppt");
				add("tmp_outtmp_hospptcost");
				add("tmp_outtmp_hospptnum");
				add("tmp_basedrug_report");
				*/
			}
		};

		// 创建备份表
		/**
		for (String table : tb) {
			String sqlDel = "DROP TABLE if EXISTS " + table + "_JY_BAK";
			shardeDBDao.jdbcTemplate.execute(sqlDel);
			//
			String sql = "create table " + table + "_JY_BAK" + " select * from " + table;
			shardeDBDao.jdbcTemplate.execute(sql);
			System.out.println("====" + sql);
		}
		*/

		// 修改备份表数据
		for (String table : tb) {
			String sql = "SHOW COLUMNS FROM " + table;
			List<Map<String, Object>> list = shardeDBDao.jdbcTemplate.queryForList(sql);
			// 把所有字段组合起来 拼接sql
			for (Map<String, Object> map : list) {
				if (map.get("field").toString().equals("mhiscode")) {
					System.out.println("------");
				}
			}
			// Insert into Table2(a, c, d) select a,c,5 from Table1
			
			

		}
	}

}
