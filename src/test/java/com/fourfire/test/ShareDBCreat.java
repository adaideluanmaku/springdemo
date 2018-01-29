package com.fourfire.test;

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
public class ShareDBCreat {

	@Autowired(required = true)
	private ShardeDBDao shardeDBDao;

	@Test
	public void c1() {
		// final String sql = MyPropertyUtil.getSql();
		// System.out.println(sql);

		final String table = "tmp_report_hosp_drug_cost";//"tmp_basedrug_report";

		List<Map<String, Object>> lst = shardeDBDao.jdbcTemplate.queryForList("show create table " + table);
		Map<String, Object> map = lst.get(0);
		String sql = (String) map.get("Create Table");
		sql = sql.replaceAll("(\r\n|\r|\n|\n\r)", " ");
		System.out.println("==============" + sql);
		int index = StringUtils.ordinalIndexOf(sql, "`", 2);
		String begin = sql.substring(0, index);
		String end = sql.substring(index);

		for (int i = 1; i <= 13; i++) {
			String sqlDel = "DROP TABLE if EXISTS " + table + String.format("_%02d", i);
			shardeDBDao.jdbcTemplate.execute(sqlDel);
			//
			String sqlnew = begin + String.format("_%02d", i) + end;
			shardeDBDao.jdbcTemplate.execute(sqlnew);
			System.out.println("====" + sqlnew);
		}
	}

}
