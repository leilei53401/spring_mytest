package com.voole.ad.loadtohdfs;

import java.io.File;

import org.apache.log4j.Logger;

public class UploadFileThread implements Runnable{
	
	private static Logger log = Logger.getLogger(UploadFileThread.class);
	
	private File  file;
	
	private String  hdfsPath;
	
	public static  long fileNum = 0;
	

	public String getHdfsPath() {
		return hdfsPath;
	}


	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}


	public UploadFileThread(File file,String hdfsPath){
		this.file = file;
		this.hdfsPath = hdfsPath;
	}
	
	@Override
	public void run() {
		boolean ifTrue = HdfsOp.uploadFile(this.file.getAbsolutePath(), this.hdfsPath,this.file.getName());
		if(ifTrue){
			log.info(this.file.getAbsolutePath()+": "+this.file.delete());
			fileNum ++;
		}
	}

}
