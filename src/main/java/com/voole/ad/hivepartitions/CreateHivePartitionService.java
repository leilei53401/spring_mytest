package com.voole.ad.hivepartitions;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.utils.GlobalProperties;

/**
 * 头一天临时建立下一天的hive目录分区
 * 
 * @author shaoyl
 *
 */
//@Component("CreateHivePartitionService")
public class CreateHivePartitionService {

	private Logger logger = LoggerFactory.getLogger(CreateHivePartitionService.class);
	
	public static final String adposType = GlobalProperties.getProperties("adposType");

	@Autowired
	public JdbcTemplate hiveJt;
	//表名，多个表用逗号隔开
	private  String tableNames;
	//从那天开始创建 -1：昨天，0：当前，1：明天
	private  int partitionDayDelay = 1;
	//创建几天，步长值
	private  int partitionDayNum = 3;
	

	/**
	 * 通用分区创建任务 ,daytime分区，格式daytime=yyyyMMdd,后三天
	 */
	public void createCommonDayTimePartition() {
		List<String> paritionsSqls = new ArrayList<String>();
		String [] tableArray = this.tableNames.split(",");
		for(String tableName : tableArray){
		    logger.info("create table ["+tableName+"] partitions!");
			DateTime currTime= new DateTime();
			DateTime startDateTime = currTime.plusDays(partitionDayDelay);
			for(int i=0;i<partitionDayNum;i++){
				DateTime createDateTime = startDateTime.plusDays(i);
				String dayPartiValue = createDateTime.toString("yyyyMMdd");
				String dayPartiInfo = "daytime="+dayPartiValue;
				String createPartitionSql = this.getAddPartitionSql(tableName, dayPartiInfo);
                logger.debug("create partition sql is  ["+createPartitionSql+"]!");
				paritionsSqls.add(createPartitionSql);
			}
		}
		//创建分区
        logger.info("start create partitions!");
		this.createPatitions(paritionsSqls);
	}
	
	/**
	 * 创建节目曝光表和库存表分区任务。一天一次
	 */
	public void createDayAndAdposTypePartition() {
		
		String [] tableArray = this.tableNames.split(",");
		for(String tableName : tableArray){
			List<String> paritionsSqls = new ArrayList<String>();
			DateTime currTime= new DateTime();
			DateTime startDateTime = currTime.plusDays(partitionDayDelay);
			for(int i=0;i<partitionDayNum;i++){
				DateTime createDateTime = startDateTime.plusDays(i);
				String dayPartiValue = createDateTime.toString("yyyyMMdd");
				String dayPartiInfo = "daytime="+dayPartiValue;
				//循环导流位类型
				String [] adpArrays = adposType.split(",");
				for(String adpType:adpArrays){
					String adpPartiInfo = "adptype="+adpType;
					String wholePartiInfo = dayPartiInfo+","+adpPartiInfo;
					String createPartitionSql = this.getAddPartitionSql(tableName, wholePartiInfo);
					paritionsSqls.add(createPartitionSql);
				}
			}	
			logger.info("start to create table ["+tableName+"] partitions !");
			this.createPatitions(paritionsSqls);
			logger.info("create table ["+tableName+"] partitions end!");
		}
	
	}
	
	/**
	 * 创建节目曝光表和库存表分区任务。（带小时，暂未考虑）
	 */
	public void createAdposTypeAndHourPartition() {
	
		String [] tableArray = this.tableNames.split(",");
		for(String tableName : tableArray){
			List<String> paritionsSqls = new ArrayList<String>();
			DateTime currTime= new DateTime();
			DateTime startDateTime = currTime.plusDays(partitionDayDelay);
			for(int i=0;i<partitionDayNum;i++){
				DateTime createDateTime = startDateTime.plusDays(i);
				String dayPartiValue = createDateTime.toString("yyyyMMdd");
				String dayPartiInfo = "daytime="+dayPartiValue;
				//循环导流位类型
				String [] adpArrays = adposType.split(",");
				for(String adpType:adpArrays){
					String adpPartiInfo = "adptype="+adpType;
					//循环小时
					for(int h=0;h<24;h++){
						String hourPartiInfo = "hourtime="+h;
						String wholePartiInfo = dayPartiInfo+","+adpPartiInfo+","+hourPartiInfo;
						String createPartitionSql = this.getAddPartitionSql(tableName, wholePartiInfo);
						paritionsSqls.add(createPartitionSql);
					}				
				}
			}
			//创建分区
			logger.info("start to create table ["+tableName+"] partitions !");
			this.createPatitions(paritionsSqls);
			logger.info("create table ["+tableName+"] partitions end!");
		}
	
	}
	
	
	private  String getAddPartitionSql(String tableName,String paritionInfo){
		return "alter table " + tableName + " add IF NOT EXISTS partition  (" + paritionInfo + ")";		
	}
	
	 
	//创建分区
	private  void createPatitions(List<String> addPatitionsSqls){
		String [] strSqlPatitions =  new String[addPatitionsSqls.size()];
		addPatitionsSqls.toArray(strSqlPatitions);
		try {
		 int[] 	resutl = hiveJt.batchUpdate(strSqlPatitions);
		 logger.info("createHivePartitions numbers = "+resutl.length);
		} catch (Exception e) {
			logger.error("create hive partition fail : ",e);
		}
		logger.info("createHivePartitions end!");
	}


	public String getTableNames() {
		return tableNames;
	}

	public void setTableNames(String tableNames) {
		this.tableNames = tableNames;
	}

	public int getPartitionDayDelay() {
		return partitionDayDelay;
	}

	public void setPartitionDayDelay(int partitionDayDelay) {
		this.partitionDayDelay = partitionDayDelay;
	}

	public int getPartitionDayNum() {
		return partitionDayNum;
	}

	public void setPartitionDayNum(int partitionDayNum) {
		this.partitionDayNum = partitionDayNum;
	}	
}
