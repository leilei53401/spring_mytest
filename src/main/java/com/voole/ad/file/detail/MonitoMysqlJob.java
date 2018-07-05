package com.voole.ad.file.detail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
public  class MonitoMysqlJob extends AbastractBaseFileJob {
	protected static Logger logger = Logger.getLogger(AbastractBaseFileJob.class);

	@Autowired
	public JdbcTemplate adSupersspJt;
	private String preSql;//商投节目sql
	private int monitorPathType;//监控路径类型
	private String monitorPath;//监控路径
	private String monitorHost;//monitor机器
	private String insertSql;//查询结果写入数据库
	private String selectOrgSql; //查询第三方监测公司名称
	
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
		DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMdd");
		DateTime localdate = DateTime.parse(time, localfromat);
		String toTime = localdate.toString("yyyy-MM-dd");
		String newSql = preSql.replaceAll("@yestoday", toTime);
		logger.info("preSql 替换后为："+newSql);
		//定义记录结果文件
		DateTime now= new DateTime();
		//String timeMin = now.toString("yyyyMMddHHmm");
		String hourTime = now.toString("yyyyMMddHH");
		String stamp = now.toString("yyyy-MM-dd HH:mm:ss");
		//获取商投节目
		List<Map<String,Object>> creativeInfoList =  adSupersspJt.queryForList(newSql);
		//获取发送第三方监测机构
        List<Map<String,Object>> orgInfoList =  adSupersspJt.queryForList(selectOrgSql);

        List<String> orgList = new ArrayList<String>();

        for(int i=0;i<orgInfoList.size();i++){
            Map<String,Object> orgInfo = orgInfoList.get(i);
            String orgName = orgInfo.get("org_name").toString();
            orgList.add(orgName);
        }
        int orgSize = orgList.size();
		//统计发送量
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
			for (int i = 0; i < orgSize; i++) {
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
				//写入数据库内容
				//创意，创意名称，监测公司，统计量，小时发送量，E区机器，正常统计，时间戳
			//	String line1 = creativeId + creativeName + domain + result + hourResult + monitorHost + "1";
				String dataSql = insertSql.replace("@creativeid", creativeId);
				String dataSqlC = dataSql.replace("@creativeName", creativeName);
				String dataSqlCN = dataSqlC.replace("@domain", orgName);
				String dataSqlCND = dataSqlCN.replace("@result", result);
				String dataSqlCNDR = dataSqlCND.replace("@hourResult", hourResult);
				String dataSqlCNDRH = dataSqlCNDR.replace("@monitorHost", monitorHost);
				String dataSqlCNDRHC= dataSqlCNDRH.replace("@countType", "0");
				String dataSqlCNDRHCS = dataSqlCNDRHC.replace("@stamp", stamp);
				adSupersspJt.execute(dataSqlCNDRHCS);
			}
		}		
		//统计失败1
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
			for (int i = 0; i < orgSize; i++) {
				String orgName = orgList.get(i);
				//指定文件路径
				String backFile ="";
				backFile = monitorPath + "FailAdUrl1/" + orgName + "/" +creativeId + "/" + time + ".txt";
				String command = "cat " + backFile + "|wc -l";
				String result = execCmd(command);// excecmd
				if("0".equals(result)){
					continue;
				}
				//统计第一次小时发送失败量
				String hourPath = "";
				hourPath = monitorPath + "FailAdUrl1/" + orgName + "/" +creativeId + "/" + time + ".txt|grep '"+hourTime+"'";
				String hourCommand = "cat " + hourPath + "|wc -l";
				String hourResult = execCmd(hourCommand); //excecmd
//				String line = "FailAdUrl1, "+timeMin+", "+creativeId+", "+creativeName+", "+domain+", "+result+ hourResult+ "\n";
				//将查询结果写入数据库，创意，创意名称，监测公司，统计量，小时发送量，E区机器，正常统计，时间戳
				String dataSql = insertSql.replace("@creativeid", creativeId);
				String dataSqlC = dataSql.replace("@creativeName", creativeName);
				String dataSqlCN = dataSqlC.replace("@domain", orgName);
				String dataSqlCND = dataSqlCN.replace("@result", result);
				String dataSqlCNDR = dataSqlCND.replace("@hourResult", hourResult);
				String dataSqlCNDRH = dataSqlCNDR.replace("@monitorHost", monitorHost);
				String dataSqlCNDRHC= dataSqlCNDRH.replace("@countType", "1");
				String dataSqlCNDRHCS = dataSqlCNDRHC.replace("@stamp", stamp);
				adSupersspJt.execute(dataSqlCNDRHCS);
			}
		}	
		
		
		//统计失败2
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
			for (int i = 0; i < orgSize; i++) {
				String orgName = orgList.get(i);
				//指定文件路径
				String backFile ="";
				backFile = monitorPath + "FailAdUrl2/" + orgName + "/" + creativeId + "/" + time + ".txt";
				String command = "cat " + backFile + "|wc -l";
				
				String result = execCmd(command);// excecmd
				
				if("0".equals(result)){
					continue;
				}
				//统计第二次小时发送失败量
				String hourPath = "";
				hourPath = monitorPath + "FailAdUrl2/" + orgName + "/" +creativeId + "/" + time + ".txt|grep '"+hourTime+"'";
				String hourCommand = "cat " + hourPath + "|wc -l";
				String hourResult = execCmd(hourCommand); //excecmd
//				String line = "FailAdUrl2, "+timeMin+", "+creativeId+", "+creativeName+", "+domain+", "+result+"\n";
				//将统计结果写入数据库
				String dataSql = insertSql.replace("@creativeid", creativeId);
				String dataSqlC = dataSql.replace("@creativeName", creativeName);
				String dataSqlCN = dataSqlC.replace("@domain", orgName);
				String dataSqlCND = dataSqlCN.replace("@result", result);
				String dataSqlCNDR = dataSqlCND.replace("@hourResult", hourResult);
				String dataSqlCNDRH = dataSqlCNDR.replace("@monitorHost", monitorHost);
				String dataSqlCNDRHC= dataSqlCNDRH.replace("@countType", "2");
				String dataSqlCNDRHCS = dataSqlCNDRHC.replace("@stamp", stamp);
				adSupersspJt.execute(dataSqlCNDRHCS);
				
			}
		}

		//统计异常
		for(Map<String,Object> creativeInfoMap:creativeInfoList){
			String creativeId = creativeInfoMap.get("creativeid").toString();
			String creativeName = creativeInfoMap.get("creativename").toString();
			//##################### 统计发送数据 #############################
			//处理域名
			for (int i = 0; i < orgSize; i++) {
				String orgName = orgList.get(i);
				//指定文件路径
				String backFile ="";
				backFile = monitorPath + "FailAdUrl-Exception/" + orgName + "/" + creativeId + "/" + time + ".txt";
				String command = "cat " + backFile + "|wc -l";
				String result = execCmd(command);// excecmd
				if("0".equals(result)){
					continue;
				}
				//统计发送异常
				String hourPath = "";
				hourPath = monitorPath + "FailAdUrl-Exception/" + orgName + "/" +creativeId + "/" + time + ".txt|grep '"+hourTime+"'";
				String hourCommand = "cat " + hourPath + "|wc -l";
				String hourResult = execCmd(hourCommand); //excecmd
				//将统计结果写入数据库
				String dataSql = insertSql.replace("@creativeid", creativeId);
				String dataSqlC = dataSql.replace("@creativeName", creativeName);
				String dataSqlCN = dataSqlC.replace("@domain", orgName);
				String dataSqlCND = dataSqlCN.replace("@result", result);
				String dataSqlCNDR = dataSqlCND.replace("@hourResult", hourResult);
				String dataSqlCNDRH = dataSqlCNDR.replace("@monitorHost", monitorHost);
				String dataSqlCNDRHC= dataSqlCNDRH.replace("@countType", "3");
				String dataSqlCNDRHCS = dataSqlCNDRHC.replace("@stamp", stamp);
				adSupersspJt.execute(dataSqlCNDRHCS);
			}
		}
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


	public void setMonitorHost(String monitorHost) {
		this.monitorHost = monitorHost;
	}

	public void setInsertSql(String insertSql) {
		this.insertSql = insertSql;
	}

	public void setAdSupersspJt(JdbcTemplate adSupersspJt) {
		this.adSupersspJt = adSupersspJt;
	}

    public String getSelectOrgSql() {
        return selectOrgSql;
    }

    public void setSelectOrgSql(String selectOrgSql) {
        this.selectOrgSql = selectOrgSql;
    }
}
