package com.fourfire.test;

import java.util.Map;

import com.fourfire.other.ScreenService;
import com.google.common.collect.Maps;
import com.medicom.modules.utils.MyUtils;
import com.medicom.modules.utils.encrypt.Cryptos;

public class RestTest {

	public static void main(String[] args) {
//		Map<String,Object> map = Maps.newHashMap();
//		map.put("type", 1);
//		map.put("tempid", 1);
//		map.put("caseid", 1);
//		map.put("mhiscode", 1);
//		map.put("prescno", 1);
//		map.put("userid", 1);
//		map.put("username", 1);
//		map.put("drugcodelist", "");
//		map.put("cer", "");
//		
		ScreenService s = new ScreenService();
//		String a = s.screenSingle(map);
		Cryptos cryptos = new Cryptos();
		String a = s.getPaModule(cryptos.aesEncrypt("1"));
		System.out.println(a);
	}
}
