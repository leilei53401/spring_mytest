package com.voole.ad.utils;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
	private static Logger logger = LoggerFactory.getLogger(DateUtils.class);

    /**
     * 获得上一个10分钟的开始时间
     *
     * @param current
     * @param min
     * @return
     */
    public static long getBeginTimeForTen(long current, int min) {
        try {
            int year = 0;
            int month = 1;
            int day = 1;
            int minute = 0;
            int hour = 0;
            Calendar calendar = Calendar.getInstance();
            if (current == 0) {
                year = calendar.get(Calendar.YEAR);
                month = calendar.get(Calendar.MONTH + 1);
                day = calendar.get(Calendar.DATE);
                hour = calendar.get(Calendar.HOUR);
                minute = calendar.get(Calendar.MINUTE);
            } else {
                Date date = new Date(current);
                year = date.getYear() + 1900;
                month = date.getMonth() + 1;
                day = date.getDate();
                hour = date.getHours();
                minute = date.getMinutes();
            }
            int newMinute = 0;
            if (minute >= 50)
                newMinute = 50 - min;
            else if (minute >= 40)
                newMinute = 40 - min;
            else if (minute >= 30)
                newMinute = 30 - min;
            else if (minute >= 20)
                newMinute = 20 - min;
            else if (minute >= 10)
                newMinute = 10 - min;
            else {
                hour = hour - 1;
                newMinute = 60 - min;
            }
            String currentTime = year + "-" + month + "-" + day + " " + hour
                    + ":" + newMinute + ":00";
            return getLongTime(currentTime);
        } catch (Exception e) {
            logger.error("获得上一个10分钟的开始时间出错:",e);
            return 0l;
        }
    }
    /**
     * 获得上一个10分钟的结束时间
     *
     * @param current
     * @param min
     * @return
     */
    public static long getEndTen(long current, int min) {
        return getBeginTimeForTen(current, min) + 10 * 60 * 1000;
    }


    /**
     * 将当前时间转换成long型时间(通用)
     *
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongTime(String time) {
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = df.parse(time);
            return date.getTime();
        } catch (Exception e) {
            logger.error("转化时间出错:",e);
            return 0l;
        }
    }


    /**
     * 后去
      * @param dt
     * @param min
     * @return
     */
    public static DateTime getEndHalf(DateTime dt, int min) {
        if(min>=30){
            dt.plusMinutes(30);
        }
        return dt;
    }


    public static void main(String[] args) {
//		String s = "http://v.admaster.com.cn/i/a65044,b963843,c3006,i0,m202,n__MAC__,0d__DEVICEID__,t__TS__,f__IP__,0a__CID__,0b__PID__,0i__SIZE__,h";
//		String s = "https://www.Admaster.com?username=goadongming";
//		String s = "http://m.super-ssp.tv/v1/a/1.gif";
//		String host = CommonUtils.getDomin(s);
//		System.out.println(host);
		
//		  String dayTime = StringUtils.substring("20170431182022", 0, 8);
//		  System.out.println(dayTime);

        DateTime currDateStartTime = new DateTime();
        //获取当前半点时刻
      /*  String  currHourTimeStr = currDateStartTime.toString("yyyy-MM-dd HH");
        String  timeMinStr = currDateStartTime.toString("mm");

        DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyy-MM-dd HH");
        DateTime currHourTime = DateTime.parse(currHourTimeStr,fromat);

        DateTime endHalfTime  =  DateUtils.getEndHalf(currHourTime,Integer.valueOf(timeMinStr));

        String halfEndStr = endHalfTime.toString("yyyy-MM-dd HH:mm:ss");
        String halfEndMomentStr = endHalfTime.toString("yyyyMMddHHmmss");*/

//
//        System.out.println(halfEndStr);
//        System.out.println(halfEndMomentStr);

        String  timeMinStr = currDateStartTime.toString("mm");
        if(Integer.valueOf(timeMinStr)>=30){
            String str =  currDateStartTime.toString("yyyy-MM-dd HH:30:00");
            String str2 =  currDateStartTime.toString("yyyyMMddHH3000");
            System.out.println(str);
            System.out.println(str2);
        }else{
            String str = currDateStartTime.toString("yyyy-MM-dd HH:00:00");
            System.out.println(str);
        }

//

	}

}
