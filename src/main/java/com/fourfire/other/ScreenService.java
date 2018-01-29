package com.fourfire.other;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.medicom.modules.utils.json.JsonMapper;

/**
 * <ul>
 * <li>项目名称：PassPaAlpha </li>
 * <li>类名称：ScreenService </li>
 * <li>类描述： PA审查调用service</li>
 * <li>创建人：zzt </li>
 * <li>创建时间：2016年12月28日 </li>
 * <li>修改备注：</li>
 * </ul>
 */
@Service
public class ScreenService {

	private static Logger logger = LoggerFactory.getLogger(ScreenService.class);

	private JsonMapper json = new JsonMapper();

	/**
	 * 
	 * <ul>
	 * <li>方法名：  screenSingle </li>
	 * <li>功能描述：调用ap审查 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年7月21日 </li>
	 * </ul> 
	 * @param params
	 * @return
	 */
	public String screenSingle(Map<String, Object> params) {
		String result = null;
		try {
			String passPath = "http://localhost:9098/passrhap";
			passPath += "/ws/paScreenSingle";
			result = restPost(passPath, json.toJson(params));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  restPost </li>
	 * <li>功能描述： </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年6月12日 </li>
	 * </ul> 
	 * @param URL
	 * @param psJSONStr
	 * @return
	 */
	public static String restPost(String url, String psJSONStr) {
		String result = "";
		logger.debug("调用passcore,pa审查 {}", psJSONStr);
		try {
			// String jsonin = "psJSONStr=" + URLEncoder.encode(psJSONStr, "UTF-8");

			CloseableHttpClient httpClient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);

			if (StringUtils.isNotBlank(psJSONStr)) {
				StringEntity stringEntity = new StringEntity(psJSONStr, HTTP.UTF_8);
				stringEntity.setContentType("application/x-www-form-urlencoded");
				httppost.setEntity(stringEntity);
			} else {
				StringEntity stringEntity = new StringEntity(HTTP.UTF_8);
				stringEntity.setContentType("application/x-www-form-urlencoded");
				httppost.setEntity(stringEntity);
			}

			HttpResponse httpResponse = httpClient.execute(httppost);
			if (httpResponse.getStatusLine().getStatusCode() == 200) {
				HttpEntity httpEntity = httpResponse.getEntity();
				result = EntityUtils.toString(httpEntity);
			} else {
				result = httpResponse.getStatusLine().toString();
			}
			httpClient.close();
		} catch (Exception e) {
			logger.error("===========begin================");
			logger.error("调用  err ws " + url, e);
			logger.error(psJSONStr);
			logger.error("===========end================");
		}
		logger.info("调用ws==" + result);
		return result;
	}

	// public static String restPost(String URL,String psJSONStr){
	// String result = "";
	// try {
	// Client client = ClientBuilder.newClient();
	// WebTarget target = client.target(URL);
	// Response response = target.request().post(Entity.entity(psJSONStr,
	// MediaType.APPLICATION_JSON_TYPE));
	// try {
	// if (response.getStatus() != 200) {
	// throw new RuntimeException("Failed with HTTP error code : " + response.getStatus());
	// }
	// result = response.readEntity(String.class);
	// } finally {
	// response.close();
	// client.close();
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// return result;
	// }

	/**
	 * 
	 * <ul>
	 * <li>方法名：  getPaModule </li>
	 * <li>功能描述： </li>
	 * <li>创建人：  zk </li>
	 * <li>创建时间：2017年1月10日 </li>
	 * </ul> 
	 * @param params
	 * @return
	 */
	public String getPaModule(String useridcode) {
		String Modules = null;
		try {
			String passPath = "http://172.18.7.118:9099/pass";
			passPath += "/reg/paModules?cer=" + useridcode;
			//
			try {
				Modules = restPost(passPath, null);
			} catch (Exception e) {
				logger.error("未能获取pass,模块,请查看passcore连接->" + e.getMessage());
			}
		} catch (Exception e) {
			logger.error("获取pass模块异常,请查看passcore连接->" + e.getMessage());
		}
		return Modules;
	}

}
