package com.voole.ad.aggre.detail.task;

import com.voole.ad.aggre.AbstractAggreTask;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *  预下载计算终端权重任务。
* @author shaoyl
* @date 2017-10-25 下午5:24:46
* @version V1.0
 */
public class PreStartAggreTask extends AbstractAggreTask {
	
	@Override
	public String doReplace() {
		DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyyMMdd");
		DateTime yestodayDateTime = DateTime.parse(this.getDate(), fromat);
        DateTime.Property p = yestodayDateTime.dayOfWeek();

//        String tagid = "8";//工作日
//        if(p.get()>5){
//            tagid = "9";//周末
//        }

		String result = this.getTaskBody().replaceAll("@yestoday",this.getDate());
		result = result.replaceAll("@tagid", p.get()+"");
		return result;
	}
	
	

}
