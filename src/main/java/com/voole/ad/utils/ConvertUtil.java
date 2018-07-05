package com.voole.ad.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class ConvertUtil {

	private Logger log = Logger.getLogger(getClass());
	
	public static String getHid(String hid) {
		hid = hid.toLowerCase();
		if (hid.length() < 12) {
			for (int i = hid.length(); i < 12; i++)
				hid += "0";
		} else if (hid.length() > 12) {
			hid = hid.substring(0, 12);
		}
		return hid;
	}
	
	
	
	/**
	 * 字符串截取,截取失败返回原值
	 * @param sourceStr
	 * @param beginIndex
	 * @param endIndex
	 * @return
	 */
	public static String subString(String sourceStr,int beginIndex, int endIndex){
		if(StringUtils.isNotBlank(sourceStr) && beginIndex < endIndex && sourceStr.length() >=endIndex){
			sourceStr = sourceStr.substring(beginIndex, endIndex);
		}
		return sourceStr;
	}
}
