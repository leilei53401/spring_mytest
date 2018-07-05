package com.voole.ad.utils;

import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {
	private static Logger logger = LoggerFactory.getLogger(CommonUtils.class);
	
	public static String getDomin(String url) {
		if (!(StringUtils.startsWithIgnoreCase(url, "http://") || StringUtils
				.startsWithIgnoreCase(url, "https://"))) {
			url = "http://" + url;
		}

		String returnVal = StringUtils.EMPTY;
		
		try {
//			URI uri = new URI(url);
//			returnVal = uri.getHost();
			URL urlObj = new URL(url);
			returnVal = urlObj.getHost();
		} catch (Exception e) {
			logger.error("get url["+url+"] domain error:",e);
		}
	/*	try {
			Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
			Matcher m = p.matcher(url);
			if (m.find()) {
				returnVal = m.group();
			}

		} catch (Exception e) {
			logger.error("get url["+url+"] domain error:",e);
		}*/
//		if ((StringUtils.endsWithIgnoreCase(returnVal, ".html") || StringUtils
//				.endsWithIgnoreCase(returnVal, ".htm"))) {
//			returnVal = StringUtils.EMPTY;
//		}
		return returnVal;
	}
	/**
	 * 解析http请求串
	 * 示例："GET /10/1.gif?uid=5df2839e8af6ccba62b8a205b5c12cea&hid=002468da15ea&oemid=30140&provinceid=43&sessionid=20170629161602&type=0&amid=1050001175&channe
lid=0&cityid=4324&ip=127.0.0.1&planid=1010001627&occurtime=1498724162454&vv=1&admt=6&uv=0&pv=0&adsdk=1&adposid=10151010&isadap=1 HTTP/1.1&starttime=20170629161602"
	 * @param line
	 * @return
	 */
	public static Map<String,String> lineMap(String line){
		try {
			String[] arr = line.replaceAll("\"","").split("\\&");
			Map<String,String> map = new HashMap<String,String>();
			for(String str : arr){
				if(str.contains("HTTP")){
					str = str.split("\\s+")[0];
				}else if(str.contains("GET ")){
					str = str.split("\\?")[1];
				}
				String[] kv = str.split("\\=");
				if(kv.length>1){
					String k = kv[0];
					String v = kv[1];
					v=v.replaceAll("%","");
					map.put(k, v);
				}else{
					//遇到值为空情况
					String k = kv[0];
					map.put(k, "");
				}
			}
			return map;
		} catch (Exception e) {
			logger.error("解析数据["+line+"]出错：",e);
			return null;
		}
	}
	
	public static void main(String[] args) {
//		String s = "http://v.admaster.com.cn/i/a65044,b963843,c3006,i0,m202,n__MAC__,0d__DEVICEID__,t__TS__,f__IP__,0a__CID__,0b__PID__,0i__SIZE__,h";
//		String s = "https://www.Admaster.com?username=goadongming";
//		String s = "http://m.super-ssp.tv/v1/a/1.gif";
//		String host = CommonUtils.getDomin(s);
//		System.out.println(host);
		
		  String dayTime = StringUtils.substring("20170431182022", 0, 8);
		  System.out.println(dayTime);
	}

}
