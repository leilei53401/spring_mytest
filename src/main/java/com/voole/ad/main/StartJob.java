package com.voole.ad.main;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * 离线计算任务入口类
* @author shaoyl
* @date 2017-4-9 上午10:56:33 
* @version V1.0
 */
public class StartJob {
	
	private static Logger logger = Logger.getLogger(StartJob.class);
	private IJobLife parseFilemysql;//统计发送数据写入mysql
	private IJobLife parseFile;//处理文件任务
	private IJobLife prepareJob;//预处理
	private IJobLife loadJob;//导入HDFS
	private IJobLife hiveAggreJob;//汇聚
	private IJobLife exportJob;//导入到Mysql
	private IJobLife extendOptJob;//扩展任务
	private int timeDelay=-1;  //延时个数
	private int timeStepSize=1;//步长
	private int timeType=0;//0，天；1，小时

	/**
	 * 依次启动job 
	 * 有依赖关系，只能顺序执行
	 */
	private void start(){
		long start = System.currentTimeMillis();
		if(1==timeType){//小时任务
			logger.info("--------------------处理小时类型数据，延时时间为【"+timeDelay+"】小时,步长为【"+timeStepSize+"】小时的数据---------------------");
			DateTime currTime= new DateTime();
			DateTime startDateTime = currTime.plusHours(timeDelay);
			
			for(int i=0;i<timeStepSize;i++){
				DateTime createDateTime = startDateTime.plusHours(i);
				String hourValue = createDateTime.toString("yyyyMMddHH");
				long dayStart = System.currentTimeMillis();
				logger.info("--------------------开始处理时间为【"+hourValue+"】的数据---------------------");
				execStatJob(hourValue);
				long dayEnd = System.currentTimeMillis();
				logger.info("--------------------结束处理时间为【"+hourValue+"】的数据,耗时["+(dayEnd-dayStart)/1000/60+"]分!---------------------");
			}
			
		}else{
			logger.info("--------------------处理天类型数据，延时时间为【"+timeDelay+"】天,步长为【"+timeStepSize+"】天的数据---------------------");
			//默认按天处理
			DateTime currTime= new DateTime();
			DateTime startDateTime = currTime.plusDays(timeDelay);
			for(int i=0;i<timeStepSize;i++){
				DateTime createDateTime = startDateTime.plusDays(i);
				String dayValue = createDateTime.toString("yyyyMMdd");
				long dayStart = System.currentTimeMillis();
				logger.info("--------------------开始处理时间为【"+dayValue+"】的数据---------------------");
				execStatJob(dayValue);
				long dayEnd = System.currentTimeMillis();
				logger.info("--------------------结束处理时间为【"+dayValue+"】的数据,耗时["+(dayEnd-dayStart)/1000/60+"]分!---------------------");
			}
		}
		
		long end = System.currentTimeMillis();
		
		logger.info("-------------------任务处理完成，耗时【"+(end-start)/1000/60+"】分!---------------------");
		
	}
	
	//执行哪一天的任务。
	private void execStatJob(String time){
		//解析本地文件任务
		if(null!=parseFile){
			logger.info("#################### 开始执行文件处理任务     #############################");
			parseFile.start();
			parseFile.process(time);
			parseFile.stop();
		}	
		//数据预处理
		if(null!=prepareJob){
			logger.info("####################开始执行数据预处理任务#############################");
			prepareJob.start();
			prepareJob.process(time);
			prepareJob.stop();
		}
		
		//导入文件到hdfs
		if(null!=loadJob){
			logger.info("####################  开始执行导入文件到hdfs任务  #############################");
			loadJob.start();
			loadJob.process(time);
			loadJob.stop();
		}
		//hive中汇聚
		if(null!=hiveAggreJob){
			logger.info("####################   开始执行汇聚任务   #############################");
			hiveAggreJob.start();
			hiveAggreJob.process(time);
			hiveAggreJob.stop();
		}
		//导入到关系型数据库
		if(null!=exportJob){
			logger.info("#################### 开始执行导入到关系型数据库任务     #############################");
			exportJob.start();
			exportJob.process(time);
			exportJob.stop();
		}		
		
		//扩展任务
		if(null!=extendOptJob){
			logger.info("#################### 开始执行导入完成后扩展任务     #############################");
			extendOptJob.start();
			extendOptJob.process(time);
			extendOptJob.stop();
		}
		if (null != parseFilemysql) {
			logger.info("#################### 开始执行统计数据发送扩展任务     #############################");
			parseFilemysql.start();
			parseFilemysql.process(time);
			parseFilemysql.stop();
		}
		
	}
	
	
	public IJobLife getParseFile() {
		return parseFile;
	}

	public void setParseFile(IJobLife parseFile) {
		this.parseFile = parseFile;
	}

	public IJobLife getPrepareJob() {
		return prepareJob;
	}


	public void setPrepareJob(IJobLife prepareJob) {
		this.prepareJob = prepareJob;
	}


	public IJobLife getLoadJob() {
		return loadJob;
	}


	public void setLoadJob(IJobLife loadJob) {
		this.loadJob = loadJob;
	}


	public IJobLife getHiveAggreJob() {
		return hiveAggreJob;
	}


	public void setHiveAggreJob(IJobLife hiveAggreJob) {
		this.hiveAggreJob = hiveAggreJob;
	}


	public IJobLife getExportJob() {
		return exportJob;
	}


	public void setExportJob(IJobLife exportJob) {
		this.exportJob = exportJob;
	}

	public IJobLife getExtendOptJob() {
		return extendOptJob;
	}

	public void setExtendOptJob(IJobLife extendOptJob) {
		this.extendOptJob = extendOptJob;
	}

	public int getTimeDelay() {
		return timeDelay;
	}


	public void setTimeDelay(int timeDelay) {
		this.timeDelay = timeDelay;
	}


	public int getTimeStepSize() {
		return timeStepSize;
	}


	public void setTimeStepSize(int timeStepSize) {
		this.timeStepSize = timeStepSize;
	}


	public int getTimeType() {
		return timeType;
	}


	public void setTimeType(int timeType) {
		this.timeType = timeType;
	}

	public void setParseFilemysql(IJobLife parseFilemysql) {
		this.parseFilemysql = parseFilemysql;
	}
	
}
