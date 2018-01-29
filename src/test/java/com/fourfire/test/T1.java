package com.fourfire.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fourfire.dealother.DrugStatDao;
import com.fourfire.dealother.RedundancyDataDealBLL;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/applicationContext.xml" })
public class T1 {


	@Autowired(required = true)
	private DrugStatDao DrugStatDao;

	@Test
	public void c1() {
		try {
			DrugStatDao.deal("2013-01-01", "2017-08-24");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
