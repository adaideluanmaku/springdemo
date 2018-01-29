package com.fourfire.test;

public class TBase {

	public static void main(String[] args) {
		String sql = "SELECT distinct concat(a.hiscode,c.caseid) AS caseid_index,  " + //
				"	       c.caseid, " + //
				"	       c.ordername, " + //
				"	       c.drugform, " + //
				"	       c.drugspec, " + //
				"		   concat(c.singledose,c.doseunit,' ',c.frequency)AS dose, " + //
				"	       c.routename, " + //
				"	       (CASE WHEN (mdoc.doctorname = '' OR mdoc.doctorname IS NULL) THEN '未知医生' ELSE mdoc.doctorname END) AS doctorname, " + //
				"	       b.startdatetime, " + //
				"	       b.enddatetime, " + //
				"	       mrqd.reviewid, " + //
				"	       mrqd.reviewname, " + //
				"	       mrqd.contentmsg " + //
				"	FROM   mc_outhosp_drugorder_detail c " + //
				"	       INNER JOIN mc_hospital_match_relation a " + //
				"	            ON a.mhiscode = c.mhiscode " + //
				"	       INNER JOIN mc_outhosp_drugorder_main b " + //
				"	            ON  c.ienddate = b.ienddate " + //
				"	            AND c.caseid = b.caseid " + //
				"	            AND c.cid = b.cid " + //
				"	            AND c.mhiscode = b.mhiscode " + //
				"	       INNER JOIN mc_dict_route mdr " + //
				"	            ON  mdr.routecode = b.routecode " + //
				"	            AND mdr.match_scheme = a.routematch_scheme " + //
				"	       INNER JOIN mc_dict_drug mdd " + //
				"	            ON  mdd.drugcode = b.ordercode " + //
				"	            AND mdd.match_scheme = a.drugmatch_scheme " + //
				"	       INNER JOIN mc_outhosp_caseid_ienddate moci ON moci.ienddate = c.ienddate AND moci.mhiscode = c.mhiscode and moci.caseid = c.caseid "
				+ //
				"	       INNER JOIN pa_table_placeholder g " + //
				"	            ON  g.caseid = moci.caseid " + //
				"	            AND g.mhiscode = moci.mhiscode " + //
				"	       LEFT JOIN mc_dict_doctor mdoc  " + //
				"	            on b.doctorcode=mdoc.doctorcode " + //
				"	            AND mdoc.match_scheme = a.doctormatch_scheme " + //
				"	       INNER JOIN mc_review_question_drugs mrqd " + //
				"	            ON c.mhiscode = mrqd.mhiscode " + //
				"	            AND c.caseid = mrqd.caseid " + //
				"	            AND c.cid = mrqd.cid " + //
				"	            AND g.templateid = mrqd.templateid " + //
				"	WHERE  b.is_use=1 AND b.ienddate BETWEEN (SELECT MIN(ienddate) FROM pa_table_placeholder) AND (SELECT MAX(ienddate) FROM pa_table_placeholder)  ";
	
		System.out.println(sql);
	}
}
