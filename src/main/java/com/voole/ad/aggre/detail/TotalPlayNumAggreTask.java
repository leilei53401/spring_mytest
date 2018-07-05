package com.voole.ad.aggre.detail;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.voole.ad.aggre.AbstractAggreTask;

/**
 * 
* @author shaoyl
* @date 2017-4-6 下午5:24:46 
* @version V1.0
 */
public class TotalPlayNumAggreTask extends AbstractAggreTask {
	
	@Override
	public String doReplace() {
		DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyyMMdd");
		DateTime yestodayDateTime = DateTime.parse(this.getDate(), fromat);
		DateTime beforeYestoday = yestodayDateTime.plusDays(-1);//获取前一天时间
		String strBeforeYestoday = beforeYestoday.toString("yyyyMMdd");
		String result = this.getTaskBody().replaceAll("@yestoday",this.getDate());
		result = result.replaceAll("@beforeyestoday", strBeforeYestoday);
		return result;
	}
	
	

}
