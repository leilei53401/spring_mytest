package com.voole.ad.loadtohdfs.monitorreport;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.file.AbastractBaseFileJob;
import com.voole.ad.main.IJobLife;

/**
 * 解析终端汇报,离线卡顿,局域网信息的任务，
 * 一天处理一次。
 * 每个job包括多个task，每个task处理不同类型的任务
* @author shaoyl
* @date 2017-6-29 下午3:46:54 
* @version V1.0
 */
public class UploadMonReportBaseJob implements IJobLife  {

    protected static Logger logger = Logger.getLogger(UploadMonReportBaseJob.class);
    //上传任务
	protected Map<String, AbstractUpLoadMonReportTask> uploadMonitorReportTaskMap;

	@Override
	public void process(String date){
		
		logger.info("################# 开始执行解析文件任务 #####################");
		long start = System.currentTimeMillis();
		int count = 0;
		//执行hive
		for (Iterator iterator = uploadMonitorReportTaskMap.keySet().iterator(); iterator.hasNext();) {           
			long startTime = System.currentTimeMillis();
	        String key = (String) iterator.next(); 
	        AbstractUpLoadMonReportTask monitorReportTask  = uploadMonitorReportTaskMap.get(key);
	        monitorReportTask.process();
	    	count++;
	    	long endTime = System.currentTimeMillis();
	    	logger.info("-------------------任务["+monitorReportTask.getTaskId()+"]执行完成，耗时【"+(endTime-startTime)+"】毫秒！-------------------");
	    }
		long end = System.currentTimeMillis();
		
		logger.info("-------------------【"+count+"】条任务执行完成，总耗时【"+(end-start)+"】毫秒！-------------------");
	
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

    public Map<String, AbstractUpLoadMonReportTask> getUploadMonitorReportTaskMap() {
        return uploadMonitorReportTaskMap;
    }

    public void setUploadMonitorReportTaskMap(Map<String, AbstractUpLoadMonReportTask> uploadMonitorReportTaskMap) {
        this.uploadMonitorReportTaskMap = uploadMonitorReportTaskMap;
    }
}
