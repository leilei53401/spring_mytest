package com.voole.ad.aggre.detail;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.voole.ad.aggre.AbstractAggreTask;
/**
 * 库存上报，小时参数替换
* @author shaoyl
* @date 2017-6-8 下午4:04:20 
* @version V1.0
 */
public class InvReportAggreTask extends AbstractAggreTask {
	
	@Override
	public String doReplace() {
	/*	DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyyMMdd");
		DateTime yestodayDateTime = DateTime.parse(this.getDate(), fromat);
		DateTime beforeYestoday = yestodayDateTime.plusDays(-1);//获取前一天时间
		String strBeforeYestoday = beforeYestoday.toString("yyyyMMdd");
		String result = this.getTaskBody().replaceAll("@yestoday",this.getDate());
		result = result.replaceAll("@beforeyestoday", strBeforeYestoday);*/
		String hourTime = this.getDate();
		String result = this.getTaskBody().replaceAll("@hourtime",hourTime);
		String daytime =  StringUtils.substring(hourTime, 0, 8);
		String lastHour =  StringUtils.substring(hourTime, 8, 10);
		 result = result.replaceAll("@today", daytime);
		 result = result.replaceAll("@lasthour", lastHour);
		return result;
	}
	
	

}
