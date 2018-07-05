package com.voole.ad.file.detail;


import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Map;

import com.voole.ad.file.AbastractBaseFileJob;
import com.voole.ad.file.ftp.FTPClientException;
import com.voole.ad.file.ftp.FTPClientTools;

/**
 * 上传tcl库存明细数据到离线服务器
 * 凌晨处理昨天的文件
* @author shaoyl
* @date 2017-6-28 下午4:06:04 
* @version V1.0
 */
public  class UploadTclInventoryFileJob extends AbastractBaseFileJob {

	
	private FTPClientTools ftpClientTools;
	private  String localPath;//本地路径配置
	private  String backupPath;//解析完文件备份路径
	private  String remotePath;//本地路径配置

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}
	/**
	 * 执行job 中 hive jdbc的内容
	 * @param date
	 */
	@Override
	public void process(String time){
		
		String dayLocalPath = localPath + File.separator + time;
		logger.info("dayLocalPath = ["+dayLocalPath+"]!");
		File f = new File(dayLocalPath);

		String[] listFiles = f.list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.endsWith(".gz")) {
					return true;
				}
				return false;
			}
		});
		
	
		if(null != listFiles && listFiles.length > 0){
			logger.info("localpath ["+dayLocalPath+"] 过滤到 .txt 文件共["+listFiles.length+"]个!");
			String dayRemotePath = this.remotePath+File.separator+time;
			logger.info("dayRemotePath = ["+dayRemotePath+"]!");
			ftpClientTools.setLocalPath(dayLocalPath);
			ftpClientTools.setRemotePath(dayRemotePath);
			try {
				Map<String,Integer> statusMap = ftpClientTools.uploadFiles(Arrays.asList(listFiles), true);
				logger.info("文件上传情况如下："+statusMap);
			} catch (FTPClientException e) {
				logger.error("上传文件出错:",e);
			}
		}else{
			logger.info("listFiles ==  null");
		}
	}
	
	public FTPClientTools getFtpClientTools() {
		return ftpClientTools;
	}

	public void setFtpClientTools(FTPClientTools ftpClientTools) {
		this.ftpClientTools = ftpClientTools;
	}

	public String getLocalPath() {
		return localPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	public String getBackupPath() {
		return backupPath;
	}

	public void setBackupPath(String backupPath) {
		this.backupPath = backupPath;
	}

	public String getRemotePath() {
		return remotePath;
	}

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}
	
}
