package com.voole.ad.loadtohdfs.detail;

import java.io.File;
import com.voole.ad.loadtohdfs.AbstractBaseLoadJob;
import com.voole.ad.loadtohdfs.HdfsOp;

/**
* @Description:导入有效排期内节目。 
* @author shaoyl
* @date 2017-4-3 下午11:21:23 
* @version V1.0
 */
public class LoadCreativeListToHdfs extends AbstractBaseLoadJob {
	private String fileName; // 文件名称

	@Override
	public void process(String time) {

		log.info("------ start load 路径为【" + srcPath + "】下名为【" + fileName
				+ "】的文件------");

		long startTime = System.currentTimeMillis();

		File filePath = new File(srcPath);

		if (!filePath.exists()) {
			log.warn("路径【" + srcPath + "】不存在！");
			return;
		}
		// 导入文件
		log.info("开始导入文件: " + fileName + "...");
		
		String wholePath = srcPath + File.separator + fileName;
		
		File srcFile = new File(wholePath);
		if (!srcFile.exists() || srcFile.length()==0) {
			log.warn("文件【" + wholePath + "】不存在！");
			return;
		}
		
		String wholeDstPath = dstPath + File.separator + fileName;

		log.info("HDFS root path为: =" + hdfsRootPath);
		log.info("wholePath=" + wholePath);
		log.info("wholeDstPath=" + wholeDstPath);

		boolean ifTrue = HdfsOp.uploadFile(true, true, wholePath, wholeDstPath,
				fileName,hadoopUser,hdfsRootPath);

		log.info("导入文件: [" + fileName + "]完成,Flag=" + ifTrue);
		Long endTime = System.currentTimeMillis();
		log.info("指标组【" + kpiType + "】导入任务成功上传到hdfs上耗时: " + (endTime - startTime) / 1000 + " s");
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
