package com.voole.ad.aggre;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import com.voole.ad.main.IJobLife;


/**
 * 汇聚hive中的数据,执行hive汇聚sql
 * @author shaoyl
 *
 */
public abstract class AbtractBaseAggreJob implements IJobLife {
	protected static Logger logger = Logger.getLogger(AbtractBaseAggreJob.class);
	protected Map<String, AbstractAggreTask> agreeTaskMap;
	@Autowired
	public JdbcTemplate hiveJt;
	
	/**
	 * 执行job 中 hive jdbc的内容
	 * @param date
	 */
	@Override
	public void process(String date){
		
		logger.info("---------开始执行汇聚任务-----------");
		long start = System.currentTimeMillis();
		int count = 0;
		
		//执行hive
		for (Iterator iterator = agreeTaskMap.keySet().iterator(); iterator.hasNext();) {           

			long startTime = System.currentTimeMillis();
	        String key = (String) iterator.next(); 
	        AbstractAggreTask aggreTask  = agreeTaskMap.get(key);
	        logger.info("hivejob ：【"+date+"%"+key+"】flag=["+aggreTask.isEnable()+"]......");
	        if(aggreTask.isEnable()){
		        aggreTask.setDate(date);
		        String sql = aggreTask.doReplace();
				logger.info("开始执行hivejob ：【"+date+"%"+key+"】的汇聚任务......");
	
				String jobName ="set mapred.job.name="+date+"%"+key;
								
				logger.info("汇聚sql语句为："+sql);
				
				try {
					hiveJt.execute(jobName);
					hiveJt.execute(sql);
				} catch (Exception e) {
					logger.error("执行执行hivejob 【"+date+"%"+key+"】的汇聚任务出错，出错sql为["+sql+"],异常信息为:",e);
				}

				count++;
				
				long endTime = System.currentTimeMillis();
				
				logger.info("hivejob : 【"+date+"%"+key+"】 执行 hive sql 操作 耗时 : ["+(endTime-startTime)/1000/60 +"]分!" );
	        }
	    }
		
		long end = System.currentTimeMillis();
		
		logger.info("-------------------hive【"+count+"】条汇聚任务执行完成，总耗时【"+(end-start)+"】毫秒！-------------------");
	
	}

	public Map<String, AbstractAggreTask> getAgreeTaskMap() {
		return agreeTaskMap;
	}

	public void setAgreeTaskMap(Map<String, AbstractAggreTask> agreeTaskMap) {
		this.agreeTaskMap = agreeTaskMap;
	}
	
}
