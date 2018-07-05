package com.voole.ad.utils;

import java.text.SimpleDateFormat;

public class DateFormatTools {
	public static ThreadLocal<SimpleDateFormat> YYYYMMDDHHMMSS = new ThreadLocal<SimpleDateFormat>() {
		public SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMddHHmmss");
		}
	};
	public static ThreadLocal<SimpleDateFormat> YYYYMMDD = new ThreadLocal<SimpleDateFormat>() {
		public SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMdd");
		}
	};
	public static ThreadLocal<SimpleDateFormat> MM = new ThreadLocal<SimpleDateFormat>() {
		public SimpleDateFormat initialValue() {
			return new SimpleDateFormat("MM");
		}
	};
}
