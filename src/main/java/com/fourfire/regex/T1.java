package com.fourfire.regex;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class T1 {
	public static void main(String[] args) {
		String sql = " SELECT a.ienddate, a.enddate, ";
		sql += " CASE WHEN b.startdatetime > '1900-01-01' THEN DATE_FORMAT(b.startdatetime,'%Y-%m-%d') ELSE a.enddate END  AS min_orderdate, ";
		sql += " CASE WHEN b.enddatetime > '1900-01-01' THEN DATE_FORMAT(b.enddatetime,'%Y-%m-%d') ELSE a.enddate END  AS max_orderdate  ";
		sql += " FROM  ( ";
		sql += "	SELECT a.ienddate, c.hiscode_user, a.mhiscode, a.caseid, a.clinicno, b.enddate ";
		sql += "	FROM mc_clinic_patient_baseinfo a ";
		sql += "	JOIN mc_clinic_patient_medinfo b ON a.caseid = b.caseid AND a.ienddate = b.ienddate AND a.mhiscode = b.mhiscode ";
		sql += "	JOIN mc_hospital_match_relation c ON a.mhiscode = c.mhiscode ";
		sql += ") a,(SELECT MIN(startdatetime)  AS startdatetime, MAX(startdatetime)  AS enddatetime,   ";
		sql += " ienddate, caseid, mhiscode FROM   mc_clinic_drugorder_main  GROUP BY ienddate, mhiscode, caseid) b  ";
		sql += " WHERE a.caseid = b.caseid  AND a.ienddate = b.ienddate   AND a.mhiscode = b.mhiscode  ";
		sql += " AND a.caseid = :caseid AND a.mhiscode = :mhiscode ";
		sql = getTableMouthSql(sql, 20170213);
		System.out.println(sql);
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  tableMouthReplace </li>
	 * <li>功能描述：分表替换策略 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年8月1日 </li>
	 * </ul> 
	 * @param str
	 * @param tableStart
	 * @param mouth
	 * @return
	 */
	public static String tableMouthReplace(String str, String tableStart, int mouth) {
		String regx = "\\s" + tableStart + "\\w*";
		Pattern pattern = Pattern.compile(regx, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(str);
		Set<String> st = Sets.newHashSet();
		while (matcher.find()) {
			st.add(matcher.group());
			System.out.println(matcher.group());
		}
		for (String s : st) {
			str = str.replaceAll(s, String.format(s + "_%02d", mouth));
		}
		return str;
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  getTableMouthSql </li>
	 * <li>功能描述：替换月表 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年8月1日 </li>
	 * </ul> 
	 * @param sql
	 * @return
	 */
	public static String getTableMouthSql(String sql, int mouth) {
		if (mouth > 19000101) {
			mouth = Integer.parseInt((mouth + "").substring(4, 6));
		}
		String[] tbStartArr = { "mc_clinic_", "mc_review_", "mc_outhosp_", "pharm_screenresults" };
		for (String tbStart : tbStartArr) {
			sql = tableMouthReplace(sql, tbStart, mouth);
		}
		return sql;
	}
}
