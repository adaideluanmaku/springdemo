package com.fourfire.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/applicationContext.xml" })
public class RedundancyTest {

	
//	@Autowired(required = true)
//	private RedundancyDataDealBLL RedundancyDataDealBLL;
//
//	@Test
//	public void c1() {
//		try {
//			RedundancyDataDealBLL.redundancyBatchDeal("", "");
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//	}
}
