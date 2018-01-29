package com.fourfire.test;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fourfire.guajie.service.ShardeDBBLL;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/applicationContext.xml" })
public class ShareDBTest {

	@Autowired(required = true)
	private ShardeDBBLL shardeDBBLL;

	@Test
	public void c1() {
		shardeDBBLL.etlCopyToShardDB(false);
	}

}
