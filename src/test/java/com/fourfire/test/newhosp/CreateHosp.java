package com.fourfire.test.newhosp;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fourfire.dealother.DrugStatDao;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/applicationContext.xml" })
public class CreateHosp {

	@Autowired(required = true)
	private DrugStatDao DrugStatDao;

	@Test
	public void c1() {
		try {
			// String sql = "insert mc_hospital
			// (hiscode,hiscode_user,stru_type,hospname,hiscode_p)values(?,?,?,?,?)";
			String[] 主城区_more = { "青羊区", "锦江区", "金牛区", "成华区", "武侯区", "温江区", "新都区", "龙泉驿区", "青白江区", "高新区", "郫县", "双流县", "新津县", "金堂县", "大邑县", "蒲江县",
					"邛崃市", "崇州市", "彭州市", "都江堰市" };
			String[] 主城区 = { "青羊区", "锦江区", "金牛区", "成华区", "武侯区", "高新区"};

			// String[] 主城区1 = { "青羊区", "锦江区"};

			List<String> lst = Lists.newArrayList();
			int index = 0;
			for (String hisname : 主城区) {
				index++;
				Map<String, Object> map = Maps.newHashMap();

				String 主城区Index = String.format("%03d", index);

				map.put("hiscode", "101013" + 主城区Index);
				map.put("hiscode_user", "HISCODE" + 主城区Index);
				map.put("stru_type", "社区医疗卫生服务中心");
				map.put("hospname", hisname);
				map.put("hiscode_p", "101013");

				DrugStatDao.insert("mc_hospital", map);
				insertM(map.get("hiscode").toString(), map.get("hiscode").toString(), hisname, "HISCODE" + 主城区Index);
				//
				map.clear();

				int max = 4;
				int min = 2;

				Random random = new Random();
				int s = random.nextInt(max) % (max - min + 1) + min;

				for (int j = 1; j <= s; j++) {
					String 医院Index = 主城区Index + String.format("%03d", j);

					String hospName_YY = hisname.substring(0,2)+"医院" + 医院Index;
					map.put("hiscode", "101013" + 医院Index);
					map.put("hiscode_user", "HISCODE" + 医院Index);
					map.put("stru_type", "社区卫生站");
					map.put("hospname", hospName_YY);
					map.put("hiscode_p", "101013" + 主城区Index);

					DrugStatDao.insert("mc_hospital", map);

					lst.add(map.get("hiscode_user").toString());

					insertM(map.get("hiscode").toString(), map.get("hiscode").toString(), hospName_YY, map.get("hiscode_user").toString());
					//
					map.clear();
				}

			}

			System.out.println(StringUtils.join(lst, ","));

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void insertM(String mhiscode, String hiscode, String hisname, String hiscode_user) {
		// if(true) {
		// return;
		// }
		Map<String, Object> map = Maps.newHashMap();
		map.put("mhiscode", mhiscode);
		map.put("hiscode", hiscode);
		map.put("hiscode_user", hiscode_user);
		map.put("hisname", hisname);
		map.put("drugmatch_scheme", "4");
		map.put("allermatch_scheme", "4");
		map.put("dismatch_scheme", "4");
		map.put("freqmatch_scheme", "4");
		map.put("routematch_scheme", "4");
		map.put("doctormatch_scheme", "4");
		map.put("oprmatch_scheme", "4");
		map.put("costitemmatch_scheme", "4");
		map.put("deptmatch_scheme", "4");
		map.put("exammatch_scheme", "4");
		map.put("labmatch_scheme", "4");
		map.put("labsubmatch_scheme", "4");
		map.put("doctorgroupmatch_scheme", "4");
		map.put("wardmatch_scheme", "4");

		DrugStatDao.insert("mc_hospital_match_relation", map);

	}

}
