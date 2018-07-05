package com.voole.ad.preplan.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class UtilityClass {
	public static int daysBetween(Date smdate,Date bdate) throws ParseException  {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");  
        smdate=sdf.parse(sdf.format(smdate));  
        bdate=sdf.parse(sdf.format(bdate));  
        Calendar cal = Calendar.getInstance();    
        cal.setTime(smdate);    
        long time1 = cal.getTimeInMillis();                 
        cal.setTime(bdate);    
        long time2 = cal.getTimeInMillis();         
        long between_days=(time2-time1)/(1000*3600*24);  
        //包含计算当天
       return Integer.parseInt(String.valueOf(between_days + 1));           
    }  
	
//	public int containWeekend(Date strtime,Date endtime) {
//		List<Date> lDate = findDates(strtime, endtime);
//		int m = 0;
//		int n = 0;
//		for (Date date : lDate) {
//			DateTime timestamp = new DateTime(date.getTime());
//			if (timestamp.getDayOfWeek() == 6 ) {
//				m++;
//			}
//			if (timestamp.getDayOfWeek() == 7) {
//				n++;
//			}
//			
//		}
//		return m+n ;
//	}
		public static List<Date> findDates(Date strtime, Date endtime) {
			List<Date> lDate = new ArrayList<Date>();
			lDate.add(strtime);
			Calendar calBegin = Calendar.getInstance();
			// 使用给定的 Date 设置此 Calendar 的时间
			calBegin.setTime(strtime);
			Calendar calEnd = Calendar.getInstance();
			// 使用给定的 Date 设置此 Calendar 的时间
			calEnd.setTime(endtime);
			// 测试此日期是否在指定日期之后
			while (endtime.after(calBegin.getTime())) {
				// 根据日历的规则，为给定的日历字段添加或减去指定的时间量
				calBegin.add(Calendar.DAY_OF_MONTH, 1);
				lDate.add(calBegin.getTime());
			}
			return lDate;
		}
}
