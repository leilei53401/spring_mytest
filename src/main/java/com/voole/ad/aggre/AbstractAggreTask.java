package com.voole.ad.aggre;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 
* @Description: 汇聚具体任务
* @author shaoyl
* @date 2017-4-6 下午4:49:58 
* @version V1.0
 */
public abstract class AbstractAggreTask implements IAggreTask{
	
	private String taskId;
	private String taskBody;//汇聚sql
	private boolean enable; //是否执行该任务
	private String date;//日期
	private String dateFormat="yyyyMMdd";//日期
	
	
	public String getTaskId() {
		return taskId;
	}




	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}




	public String getTaskBody() {
		return taskBody;
	}




	public void setTaskBody(String taskBody) {
		this.taskBody = taskBody;
	}




	public boolean isEnable() {
		return enable;
	}


	public void setEnable(boolean enable) {
		this.enable = enable;
	}




	public String getDate() {
		return date;
	}




	public void setDate(String date) {
		this.date = date;
	}



	public String getDateFormat() {
		return dateFormat;
	}


	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}


	@Override
	public String doReplace() {
		String defaultFormact= "yyyyMMdd";
		String newDate = "";
		if(dateFormat.equalsIgnoreCase(defaultFormact)){
			newDate = date;
		}else{
			DateTimeFormatter fromat = DateTimeFormat.forPattern(defaultFormact);
			DateTime dateTime = DateTime.parse(date, fromat);
			newDate = dateTime.toString(dateFormat);
		}
		return taskBody.replaceAll("@yestoday", newDate);
	}
	

}
