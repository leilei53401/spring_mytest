package com.voole.ad.file.monitorreport;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.voole.ad.utils.AdFileTools;
import com.voole.ad.utils.CommonUtils;


/**
 * 解析离线汇报数据
* @author shaoyl
* @date 2017-6-29 下午3:51:54 
* @version V1.0
 */
public  class AbstractMonitorReportTask  {
	protected static Logger logger = Logger.getLogger(AbstractMonitorReportTask.class);
	private  String taskId;//任务标识
	private  String localPath;//原始日志路径
	private  String backupPath;//解析后路径
	private  String parseKeys;//解析字段
	
	public void process(){
		logger.info("============== start parse file task ["+taskId+"] ==================");
		File f = new File(localPath);

		String[] listFiles = f.list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.endsWith(".f")) {
					return true;
				}
				return false;
			}
		});
		
	
		if(null != listFiles && listFiles.length > 0){
			logger.info("localPath ["+localPath+"] 过滤到 .f 文件共["+listFiles.length+"]个!");
			for (String processFile : listFiles) {
				logger.info("------------start parseFile ["+processFile+"] --------------");
				parseFile(processFile);
				logger.info("------------stop parseFile ["+processFile+"] --------------");
			}
		}else{
			logger.info("localPath ["+localPath+"] 下 listFiles ==  null");
		}
		
		logger.info("============== stop task ["+taskId+"] ==================");
	}
	
	
	public boolean parseFile(String inputFile) {
		

		DateTime dt = new DateTime();
		String processTime = dt.toString("yyyyMMddHHmmss");
		
		//需解析字段
		String[] keyArray  =  parseKeys.split(",");
		
		boolean result = true;
		
		File input = new File(localPath + inputFile);
		BufferedReader br = null;
		final AtomicLong at=new AtomicLong(0l);
		List<String> list = new ArrayList<String>();
		try {
			br = new BufferedReader(new FileReader(input));
			String line = null;
			StringBuilder sbResult = new StringBuilder();
			while ((line = br.readLine()) != null) {
				try {
					logger.debug("开始处理播放串["+line+"]");
					//处理starttime
					 String addParam = StringUtils.substringAfterLast(line,"starttime=");
					 String starttime = StringUtils.left(addParam, 14);
					//处理starttime
					if(StringUtils.isBlank(starttime)){
						starttime = (new DateTime()).toString("yyyyMMddHHmmss");
					}
						
					String date = starttime.substring(0,8);
					
					//截取request串。
					String param = StringUtils.substringBetween(line,"?","HTTP");
					if(StringUtils.isBlank(param)){
						param = line;
					}
					param = StringUtils.trimToEmpty(param);
					
					Map<String, String> lineMap = CommonUtils.lineMap(param);
					
					if(lineMap == null){
						logger.error("播放串["+line+"]解析异常,lineMap==null!");
						continue;
					}
					
					//补充固定数据
					lineMap.put("starttime", starttime);
					lineMap.put("daytime", date);
	
					for(String key : keyArray){
						String val = lineMap.get(key);
						if(StringUtils.isNotBlank(val) && !"null".equalsIgnoreCase(val)){
							sbResult.append(val);
						}
						sbResult.append(",");
					}
					
					String out = sbResult.toString();
					out =  StringUtils.substring(out, 0, out.length()-1); 
					
					//写到文件
					String outFileName = date + "_" + processTime + ".txt";
					
					AdFileTools.writeLineToFile(backupPath, outFileName, out+"\n", true);
					
					sbResult.setLength(0);
					if(at.incrementAndGet()%5000==0){
						logger.error("处理数据["+at.get()+"]条!");
					}
			
				} catch (Exception e) {
					logger.error("处理数据["+line+"]异常:",e);
					continue;
				}
			}


			br.close();

            logger.info("prcessFile ["+inputFile+"]处理完成，解析数["+at.get()+"]条！");
		
		}catch (Exception ex) {
			logger.error("prcessFile ["+inputFile+"] error:" , ex);
			result = false;
		} finally {
			try {
				if (br != null){
					br.close();
					br = null;
				 }
				
				if(result){
					if (input != null && input.exists()) {
	//					input.delete();
						//解析完备份
						String backupName = StringUtils.replace(inputFile, ".f", ".COMPLETED");
						String backFilePath = "";
						if(localPath.endsWith("/")){
							backFilePath = localPath+backupName;
						}else{
							backFilePath = localPath+File.separator+backupName;
						}
						File backFile =  new File(backFilePath);
						boolean flag = input.renameTo(backFile);
						logger.info("备份文件["+inputFile+"]到["+localPath+"]下文件名为["+backupName+"]完成，flag="+flag);
					}
				}				
			
			} catch (Exception e) {
				logger.info("流关闭异常", e);
			}
		}
		return result;
	}
	
	public String getTaskId() {
		return taskId;
	}


	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}


	public String getParseKeys() {
		return parseKeys;
	}

	public void setParseKeys(String parseKeys) {
		this.parseKeys = parseKeys;
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
    
	
	
}
