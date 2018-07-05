package com.voole.ad.file.detail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.file.AbastractBaseFileJob;

/**
 * 监控商投发送第三方数据记录到文件
 * @author shaoyl
 *
 */
public  class MonitoBusiJob extends AbastractBaseFileJob {
	protected static Logger logger = Logger.getLogger(AbastractBaseFileJob.class);

	@Autowired
	public JdbcTemplate adSupersspJt;
	private  String preSql;//商投节目sql
	private  int monitorPathType;//监控路径类型
	private  String monitorPath;//监控路径
	private  String recordPath;//解析完文件备份路径
	private  String monitorDomain;//第三方域名,多个逗号分隔

    private  String orgSql;//第三方监测机构查询sql
	
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
	 * @param time
	 */
	@Override
	public void process(String time){
		
		logger.info("--------------进入monitorAgent监控统计处理-------------------------");
//		long start  = System.currentTimeMillis();
		DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMdd");
		DateTime localdate = DateTime.parse(time, localfromat);
		String toTime = localdate.toString("yyyy-MM-dd");
		String newSql = preSql.replaceAll("@yestoday", toTime);
		logger.info("preSql 替换后为："+newSql);
		//定义记录结果文件
		String recordFile = recordPath + time + ".txt";
		DateTime now= new DateTime();
		String timeMin = now.toString("yyyyMMddHHmm");
		String hourTime = now.toString("yyyyMMddHH");
		String title = "##########################  "+ timeMin +"  ##################################\n";
		writeLineToFile(recordFile,title,true);
        //获取发送第三方监测机构
        List<Map<String,Object>> orgInfoList =  adSupersspJt.queryForList(orgSql);

        List<String> orgList = new ArrayList<String>();

        for(int i=0;i<orgInfoList.size();i++){
            Map<String,Object> orgInfo = orgInfoList.get(i);
            String orgName = orgInfo.get("org_name").toString();
            orgList.add(orgName);
        }

        int orgSize = orgList.size();

		//获取商投节目
		List<Map<String,Object>> creativeInfoList =  adSupersspJt.queryForList(newSql);
		
		//统计发送量
		String sendTitle = "-------------------------------- SendCount  ---------------------------\n";
		writeLineToFile(recordFile,sendTitle,true);		
		
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
//			String[] domainArray =  StringUtils.trim(monitorDomain).split(",");
//			for(String domain: domainArray){

            for(int i=0;i<orgSize;i++){
                String orgName = orgList.get(i);
				//指定文件路径
				String backFile ="";
				if(monitorPathType==1){//分日目录
					 backFile = monitorPath + time +"/" + creativeId + "/" + orgName + "/" + time + ".txt";
				}else{
					 backFile = monitorPath + creativeId + "/" + orgName + "/" + time + ".txt";
				}
				String command = "cat " + backFile + "|wc -l";
				
				String result = execCmd(command);// excecmd
				
				if("0".equals(result)){
					continue;
				}
				
				
				//统计小时
				String hourPath = "";
				if(monitorPathType==1){//分日目录
					hourPath = monitorPath + time +"/" + creativeId + "/" + orgName + "/" + time + ".txt|grep '"+hourTime+"'";
				}else{
					hourPath = monitorPath + creativeId + "/" + orgName + "/" + time + ".txt|grep '"+hourTime+"'";
				}
				
				String hourCommand = "cat " + hourPath + "|wc -l";
				
				String hourResult = execCmd(hourCommand); //excecmd
				
				String line = "SendCount, "+timeMin+", "+creativeId+", "+creativeName+", "+orgName+", "+result+", "+hourResult+"\n";
				
				writeLineToFile(recordFile,line,true);
				
			}
		}		
		

		//统计失败1
		String failTitle1 = "\n-------------------------------- FailAdUrl1  ---------------------------\n";
		writeLineToFile(recordFile,failTitle1,true);		
		
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
//			String[] domainArray =  StringUtils.trim(monitorDomain).split(",");
//			for(String domain: domainArray){
            for(int i=0;i<orgSize;i++){
                String orgName = orgList.get(i);
				//指定文件路径
				String backFile ="";
//				if(monitorPathType==1){//分日目录
//					 backFile = monitorPath + time +"/" + creativeId + "/" + domain + "/" + time + ".txt";
//				}else{
					 backFile = monitorPath + "FailAdUrl1/" + orgName + "/" +creativeId + "/" + time + ".txt";
//				}
				String command = "cat " + backFile + "|wc -l";
				
				String result = execCmd(command);// excecmd
				
				if("0".equals(result)){
					continue;
				}
				
				String line = "FailAdUrl1, "+timeMin+", "+creativeId+", "+creativeName+", "+orgName+", "+result+"\n";
				
				writeLineToFile(recordFile,line,true);
				
			}
		}	
		
		
		//统计失败2
		String failTitle2 = "\n-------------------------------- FailAdUrl2  ---------------------------\n";
		writeLineToFile(recordFile,failTitle2,true);		
		
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
//			String[] domainArray =  StringUtils.trim(monitorDomain).split(",");
//			for(String domain: domainArray){
            for(int i=0;i<orgSize;i++){
                String orgName = orgList.get(i);
				//指定文件路径
				String backFile ="";
//						if(monitorPathType==1){//分日目录
//							 backFile = monitorPath + time +"/" + creativeId + "/" + domain + "/" + time + ".txt";
//						}else{
					 backFile = monitorPath + "FailAdUrl2/" + orgName + "/" +creativeId + "/" + time + ".txt";
//						}
				String command = "cat " + backFile + "|wc -l";
				
				String result = execCmd(command);// excecmd
				
				if("0".equals(result)){
					continue;
				}
				
				String line = "FailAdUrl2, "+timeMin+", "+creativeId+", "+creativeName+", "+orgName+", "+result+"\n";
				
				writeLineToFile(recordFile,line,true);
				
			}
		}

		//统计异常
		String failTitleExp = "\n-------------------------------- FailAdUrlException  ---------------------------\n";
		writeLineToFile(recordFile,failTitleExp,true);		
		
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
//			String[] domainArray =  StringUtils.trim(monitorDomain).split(",");
//			for(String domain: domainArray){

            for(int i=0;i<orgSize;i++){
                String orgName = orgList.get(i);
				//指定文件路径
				String backFile ="";
//								if(monitorPathType==1){//分日目录
//									 backFile = monitorPath + time +"/" + creativeId + "/" + domain + "/" + time + ".txt";
//								}else{
					 backFile = monitorPath + "FailAdUrl-Exception/" + orgName + "/" +creativeId + "/" + time + ".txt";
//								}
				String command = "cat " + backFile + "|wc -l";
				
				String result = execCmd(command);// excecmd
				
				if("0".equals(result)){
					continue;
				}
				
				String line = "FailAdUrlException, "+timeMin+", "+creativeId+", "+creativeName+", "+orgName+", "+result+"\n";
				
				writeLineToFile(recordFile,line,true);
				
			}
		}
		
		String foot = "==========================  done  ===============================\n\n\n\n";
		writeLineToFile(recordFile,foot,true);
		
		logger.info("-------------结束入monitorAgent监控统计处理-------------------------");
	}
	
	private String execCmd(String cmd){
		String result = "0"; 
		try {
			Runtime rt = Runtime.getRuntime(); 
			String[] cmdA = { "/bin/sh", "-c", cmd };
			Process p = rt.exec(cmdA);
			BufferedReader  in = new BufferedReader(new InputStreamReader(p.getInputStream())); 
			String str = null; 
			while ((str = in.readLine()) != null) {
				result = str.trim();
			   logger.info("exec cmd ["+cmd+ "] result is : "+result);
			}
		} catch (Exception e) {
			logger.error("exec cmd error:",e);
		}
		logger.info(" result is : "+result);
		return result;
	    
	}
	
	private void writeLineToFile(String filePath, String line, boolean appead){
		File file = FileUtils.getFile(new File(filePath));
		try {
			FileUtils.writeStringToFile(file, line, appead);
		} catch (IOException e) {
			logger.error("writeLineToFile to file exception:",e);
		}
	}

	public String getPreSql() {
		return preSql;
	}

	public void setPreSql(String preSql) {
		this.preSql = preSql;
	}

	public int getMonitorPathType() {
		return monitorPathType;
	}

	public void setMonitorPathType(int monitorPathType) {
		this.monitorPathType = monitorPathType;
	}

	public String getMonitorPath() {
		return monitorPath;
	}

	public void setMonitorPath(String monitorPath) {
		this.monitorPath = monitorPath;
	}

	public String getRecordPath() {
		return recordPath;
	}

	public void setRecordPath(String recordPath) {
		this.recordPath = recordPath;
	}

	public String getMonitorDomain() {
		return monitorDomain;
	}

	public void setMonitorDomain(String monitorDomain) {
		this.monitorDomain = monitorDomain;
	}

    public String getOrgSql() {
        return orgSql;
    }

    public void setOrgSql(String orgSql) {
        this.orgSql = orgSql;
    }
}
