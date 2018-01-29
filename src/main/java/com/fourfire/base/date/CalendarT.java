package com.fourfire.base.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.medicom.modules.exceptions.BusinessException;
import com.medicom.modules.utils.date.DateStyle;

public class CalendarT {

	public static void main(String[] args) throws Exception {
		List<Integer> ms = getIntervalMonths("2015-03-31 12:12:12", "2016-05-31 12:12:12", "yyyy-MM-dd");
		for (int i = 0; i < ms.size(); i++) {
			System.out.println(ms.get(i));
		}
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  getIntervalMonths </li>
	 * <li>功能描述：计算2个日期相差具体月份集合,已去重 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年8月10日 </li>
	 * </ul> 
	 * @param minDate
	 * @param maxDate
	 * @return
	 * @throws ParseException
	 */
	public static List<Integer> getIntervalMonths(String minDate, String maxDate, String pattern) throws Exception {
		List<Integer> months = new ArrayList<Integer>();
		List<String> monthsAll = getMonthBetween(minDate, maxDate, pattern);
		for (String month : monthsAll) {
			Integer m = null;
//			switch (pattern) {
//			case "yyyyMMdd":
//				m = Integer.parseInt(StringUtils.substring(month, 4, 6));
//				break;
//			case "yyyy-MM-dd":
//				m = Integer.parseInt(StringUtils.substring(month, 5, 7));
//				break;
//			default:
//				throw new BusinessException("错误参数类型" + pattern);
//			}

			if (!months.contains(m)) {
				months.add(m);
			}
		}
		return months;
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  getMonthBetween </li>
	 * <li>功能描述：获取2个日期间 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年8月10日 </li>
	 * </ul> 
	 * @param minDate
	 * @param maxDate
	 * @return
	 * @throws ParseException
	 */
	public static List<String> getMonthBetween(String minDate, String maxDate, String pattern) throws ParseException {
		ArrayList<String> result = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);// 格式化为年月

		Calendar min = Calendar.getInstance();
		Calendar max = Calendar.getInstance();

		min.setTime(sdf.parse(minDate));
		min.set(min.get(Calendar.YEAR), min.get(Calendar.MONTH), 1);

		max.setTime(sdf.parse(maxDate));
		max.set(max.get(Calendar.YEAR), max.get(Calendar.MONTH), 2);

		Calendar curr = min;
		while (curr.before(max)) {
			System.out.println(sdf.format(curr.getTime()));
			result.add(sdf.format(curr.getTime()));
			curr.add(Calendar.MONTH, 1);
		}
		return result;
	}

	public final static String addMonthsToDate(Date date, int months) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.MONTH, months);// 加 months 个月

		c.set(Calendar.DAY_OF_MONTH, 1);// 设置月份的月初

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 设置时间格式
		String defaultStartDate = sdf.format(c.getTime()); // 格式化前3月的时间

		return defaultStartDate;
	}

}
