package com.voole.ad.loadtohdfs;

import java.io.File;
import java.io.FileFilter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.main.IJobLife;


/**
* @Description: 导入文件到hdfs抽象类
* 此类默认导入前一天日期的文件，文件名称上带日期
* 其他具体导入类，继承该类
* 如果有特殊需求重写 startLoad() 方法
* @author shaoyl 
* @date 2017-4-1 下午5:30:14 
* @version V1.0
 */
public abstract class AbstractBaseLoadJob implements IJobLife {
	 
	protected static Logger log = Logger.getLogger(AbstractBaseLoadJob.class);
	
	protected String kpiType;//指标组
	
	protected String srcPath; //本地保存文件路径
	
	//hdfs 相关
	protected String hadoopUser;
	
	protected String hdfsRootPath;
	
	protected String dstPath; //hdfs路径
	//示例：/home/hdp/hadoop/hive-0.12.0-cdh5.0.0/warehouse/aclog_config.db/aclog_bell_detail/daytime=20140921/hourtime=2014092123
	
	protected String addPartSql;//不同的指标组配置不同的添加分区sql
	

	//线程参数
	protected int corePoolSize;
	protected int maximumPoolSize;
	
	//空闲的线程数
	protected int  activeAlive = 0 ;
	
	//文件过滤参数
	protected String fileFilterParam;
	
	protected String filter;//过滤字符串
	
	@Override
	public void process(String time){
		log.info("------ start load 路径为【"+srcPath+"】下指标组为【"+kpiType+"】日期为【"+time+"】的文件------");
		
		long startTime = System.currentTimeMillis();	
		
		filter = fileFilterParam.replaceAll("yyyyMMdd", time);
	
		log.info("文件过滤参数为【"+filter+"】");
		
		//初始化线程池
		BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
	    ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, activeAlive, TimeUnit.DAYS, queue);

		File filePath = new File(srcPath);
		File[] files = filePath.listFiles(
				new FileFilter() {
			          public boolean accept(File file) {
			           return file.getName().indexOf(filter)>-1;
			          }
				});
			
			log.info("指标组【"+kpiType+"】时间【"+time+"】过滤到【"+files.length+"】个文件！");
			
		String hdfsPath = "";
		if(files.length>0){
			//创建hdfs目录
			hdfsPath = getWholHdfsPath(time);
			log.info("开始创建HDFS目录:"+hdfsPath);
			HdfsOp.createDir(hdfsPath);
			//创建表分区
			String hdfsSql = getAddPartitionSql(hdfsPath,time);
//			DbUtil.addPartition(hdfsSql,dbEnv);
		}else{
			log.warn("无文件导入！");
		}
			
		//导入文件
		for(File file:files){
			
			String fileName = file.getName();
			 
			if(file.isFile() && file.length() > 0){	
				
				log.info("开始导入文件: "+ fileName +"...");
				
				executor.execute(new UploadFileThread(file,hdfsPath));
				
			}
			
		}
			
	 executor.shutdown();
	 try {
		executor.awaitTermination(100,TimeUnit.HOURS);
	} catch (InterruptedException e) {
		log.error(e);
	}
	Long endTime = System.currentTimeMillis();
	log.info("指标组【"+kpiType+"】导入任务成功上传 "+UploadFileThread.fileNum+" 个日志文件到hdfs上耗时: "+(endTime-startTime)/1000+" s");
		
	}
	
	
	/**
	 * 获取hdfs上dns目录 
	 * @param date
	 */
	protected String getWholHdfsPath(String day){
		String hdfsPath = dstPath.replaceAll("\\{time\\}",day);
		return hdfsPath;
	}
	
	/**
	 * 获取add分区sql
	 * @param hdfspath
	 * @param day
	 * @param hou
	 * @return
	 */
	protected String getAddPartitionSql(String hdfspath,String day){
		String sql = addPartSql.replaceAll("\\{time\\}", day).replaceAll("\\{hdfspath\\}", hdfspath);
		return sql;
	}


	public String getKpiType() {
		return kpiType;
	}


	public void setKpiType(String kpiType) {
		this.kpiType = kpiType;
	}


	public String getSrcPath() {
		return srcPath;
	}

	public void setSrcPath(String srcPath) {
		this.srcPath = srcPath;
	}


	public String getHadoopUser() {
		return hadoopUser;
	}


	public void setHadoopUser(String hadoopUser) {
		this.hadoopUser = hadoopUser;
	}


	public String getHdfsRootPath() {
		return hdfsRootPath;
	}


	public void setHdfsRootPath(String hdfsRootPath) {
		this.hdfsRootPath = hdfsRootPath;
	}


	public String getDstPath() {
		return dstPath;
	}


	public void setDstPath(String dstPath) {
		this.dstPath = dstPath;
	}



	public int getCorePoolSize() {
		return corePoolSize;
	}


	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}


	public int getMaximumPoolSize() {
		return maximumPoolSize;
	}


	public void setMaximumPoolSize(int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
	}


	public int getActiveAlive() {
		return activeAlive;
	}


	public void setActiveAlive(int activeAlive) {
		this.activeAlive = activeAlive;
	}
	
	
	public String getAddPartSql() {
		return addPartSql;
	}

	public void setAddPartSql(String addPartSql) {
		this.addPartSql = addPartSql;
	}

	public String getFileFilterParam() {
		return fileFilterParam;
	}


	public void setFileFilterParam(String fileFilterParam) {
		this.fileFilterParam = fileFilterParam;
	}


	public static void main(String[] args){
		String s = "/home/hdp/hadoop/hive-0.12.0-cdh5.0.0/warehouse/aclog_config.db/aclog_bell_detail/daytime=111/hourtime=222";
		String s2 = "alter table aclog_bell_usage add IF NOT EXISTS partition (daytime={day},hourtime={hou}) location '{hdfspath}'";
		s2 = s2.replaceAll("\\{hdfspath\\}", s);
		
		System.out.println(s2);
	}

}
