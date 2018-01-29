package com.fourfire.dealother;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fourfire.dealother.DrugStatDao;
import com.fourfire.dealother.McClinicDao;
import com.fourfire.dealother.OtherDealDao;
import com.fourfire.dealother.RationalDrugStatDao;
import com.fourfire.dealother.UnAuthDrugStatDao;

/**
 * 
 * <ul>
 * <li>项目名称：PASS_RH_AP_V5.0 </li>
 * <li>类名称：  RedundancyDataDealBLL </li>
 * <li>类描述：冗余数据处理   </li>
 * <li>创建人：jy </li>
 * <li>创建时间：2017年6月22日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Service
public class RedundancyDataDealBLL {

	@Autowired
	private UnAuthDrugStatDao unAuthDrugStatDao;
	@Autowired
	private McClinicDao mcClinicDao;
	@Autowired
	private DrugStatDao drugStatDao;
	@Autowired
	private RationalDrugStatDao rationalDrugStatDao;
	@Autowired
	private OtherDealDao otherDealDao;
	/**
	 * 
	 * <ul>
	 * <li>方法名：  redundancyBatchDeal </li>
	 * <li>功能描述：冗余数据批量处理 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月23日 </li>
	 * </ul>
	 */
	public void redundancyBatchDeal(String startdate, String enddate) {
		
//		// 5 合理用药指标统计 
//		rationalDrugStatDao.deal(startdate, enddate);
//		
//		// 6 药品统计分析
//		drugStatDao.deal(startdate, enddate);
//		
//		// 7 其它统计(7.1 越权用药统计表)
//		unAuthDrugStatDao.deal(startdate,enddate);
		
		// 门诊数据处理
		mcClinicDao.deal(startdate,enddate);
		
		//其他处理
//		otherDealDao.deal();
	}
}
