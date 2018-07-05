package com.voole.ad.loadtohdfs.monitorreport;

import java.io.File;
import org.apache.log4j.Logger;
import com.voole.ad.loadtohdfs.HdfsOp;

/**
 * 上传离线计算结果文件到hdfs
* @author shaoyl
* @date 2017-6-29 下午5:06:57 
* @version V1.0
 */

public class AbstractUpLoadMonReportTask {
	
	protected static Logger logger = Logger.getLogger(AbstractUpLoadMonReportTask.class);
	
	protected String taskId;//指标组
	
	protected String srcPath; //本地保存文件路径
	
	//hdfs 相关
	protected String hadoopUser;
	
	protected String hdfsRootPath;
	
	protected String dstPath; //hdfs路径
	
	public void process(){
		logger.info("============== start task ["+taskId+"] ==================");
		File f = new File(srcPath);

		String[] listFiles = f.list();
		
		if(null != listFiles && listFiles.length > 0){
			logger.info("localPath ["+srcPath+"] 过滤到 .f 文件共["+listFiles.length+"]个!");
			for (String processFile : listFiles) {
				logger.info("------------start parseFile ["+processFile+"] --------------");
				upLoadFile(processFile);
				logger.info("------------stop parseFile ["+processFile+"] --------------");
			}
		}else{
			logger.info("localPath ["+srcPath+"] 下 listFiles ==  null");
		}
		
		logger.info("============== stop task ["+taskId+"] ==================");
	}


	public void upLoadFile(String processFile) {

		logger.info("------ start load 路径为【" + srcPath + "】下名为【" + processFile + "】的文件------");

		long startTime = System.currentTimeMillis();

		File filePath = new File(srcPath);

		if (!filePath.exists()) {
			logger.warn("路径【" + srcPath + "】不存在！");
			return;
		}
		// 导入文件
		logger.info("开始导入文件: " + processFile + "......");
		
		String wholePath = srcPath + File.separator + processFile;
		
		File srcFile = new File(wholePath);
		if (!srcFile.exists() || srcFile.length()==0) {
			logger.warn("文件【" + wholePath + "】不存在！");
			return;
		}
		
		//处理日期目录
		String daytime = "daytime="+processFile.substring(0,processFile.indexOf("_"));
		
		String wholeDstPath = dstPath + File.separator + daytime + File.separator + processFile;

		logger.info("HDFS root path为: =" + hdfsRootPath);
		logger.info("wholePath=" + wholePath);
		logger.info("wholeDstPath=" + wholeDstPath);

		boolean ifTrue = HdfsOp.uploadFile(true, true, wholePath, wholeDstPath,
				processFile,hadoopUser,hdfsRootPath);

		logger.info("导入文件: [" + processFile + "]完成,Flag=" + ifTrue);
		Long endTime = System.currentTimeMillis();
		logger.info("指标组【" + taskId + "】导入任务成功上传到hdfs上耗时: " + (endTime - startTime) / 1000 + " s");
	}


	public String getTaskId() {
		return taskId;
	}


	public void setTaskId(String taskId) {
		this.taskId = taskId;
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
	
	
	

}
