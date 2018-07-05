package com.voole.ad.aggre.detail;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.aggre.AbstractAggreTask;
import com.voole.ad.main.IJobLife;
import com.voole.ad.utils.GlobalProperties;

public class ArchiceSmallFileJob implements IJobLife{
	protected static Logger logger = Logger.getLogger(ArchiceSmallFileJob.class);
	protected Map<String, AbstractAggreTask> archiveTaskMap;
	@Autowired
	public JdbcTemplate hiveJt;
	public static final String adposType = GlobalProperties.getProperties("adposType");
	
	@Override
	public void process(String date) {
		logger.info("=============================开始执行合并HDFS小文件任务=============================");
		long start = System.currentTimeMillis();
		int count = 0;
		// 执行hive
		for (Iterator<String> iterator = archiveTaskMap.keySet().iterator(); iterator.hasNext();) {
			long startTime = System.currentTimeMillis();
			String key = (String) iterator.next();
			AbstractAggreTask aggreTask = archiveTaskMap.get(key);
			logger.info("hivejob ：【" + date + "%" + key + "】flag=[" + aggreTask.isEnable() + "]......");
			if (aggreTask.isEnable()) {
				aggreTask.setDate(date);
				String party1Sql = aggreTask.doReplace();
				String[] adptpyes = adposType.split(",");
				for (int i = 0; i < adptpyes.length; i++) {
					String party2Sql = party1Sql.replace("@adptype", adptpyes[i]);
					logger.info("开始执行hivejob ：【" + date + "%" + key + "】的合并文件任务......");

					String jobName = "set mapred.job.name=" + date + "%" + key;

					logger.info("执行sql语句为：" + party2Sql);

					try {
						hiveJt.execute(jobName);
						hiveJt.execute(party2Sql);
					} catch (Exception e) {
						logger.error("执行执行hivejob 【" + date + "%" + key + "】的合并文件任务出错，出错sql为[" + party2Sql + "],异常信息为:",e);
					}

				}
				count++;

				long endTime = System.currentTimeMillis();

				logger.info("hivejob : 【" + date + "%" + key + "】 执行 hive sql 操作 耗时 : ["
						+ (endTime - startTime) / 1000 / 60 + "]分!");
			}
		}

		long end = System.currentTimeMillis();

		logger.info("-------------------hive【" + count + "】条合并文件任务执行完成，总耗时【" + (end - start) + "】毫秒！-------------------");
	}


	@Override
	public void start() {
		
	}


	@Override
	public void stop() {
		
	}


	public Map<String, AbstractAggreTask> getArchiveTaskMap() {
		return archiveTaskMap;
	}


	public void setArchiveTaskMap(Map<String, AbstractAggreTask> archiveTaskMap) {
		this.archiveTaskMap = archiveTaskMap;
	}

	

}
