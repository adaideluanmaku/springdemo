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

/**
 * 
 * <ul>
 * <li>项目名称：springdemo </li>
 * <li>类名称：  ShareDBBack </li>
 * <li>类描述：月表写回到主表   </li>
 * <li>创建人：jy </li>
 * <li>创建时间：2017年7月31日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/applicationContext.xml" })
public class ShareDBBack {

	@Autowired(required = true)
	private ShardeDBDao shardeDBDao;

	@Test
	public void c1() {
		shardeDBDao.jdbcTemplate.queryForList("select 250");
	}

}
