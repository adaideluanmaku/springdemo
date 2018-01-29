package com.fourfire.test.redis;

import java.util.List;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/applicationContext.xml" })
public class RedisT1 {

	@Resource(name = "redisTemplate")
	private RedisTemplate<String, String> redisTemplate;

	@Resource(name = "redisStrTemplate")
	private StringRedisTemplate redisStrTemplate;

	/**审查结果 */
	public static String SA_SCREENRESULTS_PREFIX = "X_SA_SR";// ScreenResult

	/**pa审查结果 */
	public static final String PA_SCREENRESULTS = "PA_SCREENRESULT_LIST";

	@Test
	public void c1() {
		long len = 0;

		ListOperations<String, String> ops = redisStrTemplate.opsForList();
		len = ops.size(PA_SCREENRESULTS);
		System.out.println(len);
		//
		//redisStrTemplate.delete(PA_SCREENRESULTS);

		//len = ops.size(PA_SCREENRESULTS);
		//System.out.println(len);
	}

}
