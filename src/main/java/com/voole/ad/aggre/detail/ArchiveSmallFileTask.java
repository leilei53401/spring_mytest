package com.voole.ad.aggre.detail;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.voole.ad.aggre.AbstractAggreTask;

public class ArchiveSmallFileTask extends AbstractAggreTask{
	/**
	 * 创建合并文件的表分区/daytime/adptype
	 */
	@Override
	public String doReplace() {
		DateTimeFormatter dFormat = DateTimeFormat.forPattern("yyyyMMdd");
		DateTime daytime = DateTime.parse(this.getDate(), dFormat);
		String rpDaytime = daytime.toString("yyyyMMdd");
		String sqlPart1 = this.getTaskBody().replaceAll("@daytime", rpDaytime);
		return sqlPart1;
	}
	
}
