package com.voole.ad.file.detail;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import com.voole.ad.file.AbastractBaseFileJob;

/**
 * 解析文件同步离线报表数据
 * @author shaoyl
 *
 */
public  class SyncReportFileJob extends AbastractBaseFileJob {
	protected static Logger logger = Logger.getLogger(AbastractBaseFileJob.class);

	@Autowired
	public JdbcTemplate adSupersspJt;
	
	private  String localPath;//本地路径配置
	private  String backupPath;//解析完文件备份路径
	//导出没批次数量
	protected int bachSize = 100;
	
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
			logger.info("localpath ["+localPath+"] 过滤到 .txt 文件共["+listFiles.length+"]个!");
			for (String processFile : listFiles) {
				parseFile(processFile);
			}
		}else{
			logger.info("listFiles ==  null");
		}
		
	}
	
	
	public boolean parseFile(String inputFile) {
		boolean result = true;
		File input = new File(localPath + inputFile);
		BufferedReader br = null;
		final AtomicLong at=new AtomicLong(0l);
		List<String> list = new ArrayList<String>();
		try {
			br = new BufferedReader(new FileReader(input));
			String line = null;
	//		PlayLogBean playLogBean = null;
			while ((line = br.readLine()) != null) {
				try {
					logger.debug("开始处理播放串["+line+"]");
					
					//截取request串。
					String param = StringUtils.substringBetween(line,"?","HTTP");
					if(StringUtils.isBlank(param)){
						param = line;
					}
					param = StringUtils.trimToEmpty(param);
					
					 String tableName = StringUtils.substringBefore(StringUtils.substringAfter(param,"tb="), "&");
					 String values = StringUtils.substringAfter(param, "&");
					 
					 String insertSql = parseDataToSql(tableName,values);
					 logger.debug("insertSql===["+line+"]");
					 list.add(insertSql);
					 
					if(at.incrementAndGet()%bachSize==0){
						doExport(list);
						list.clear();
					}
			
				} catch (Exception e) {
					logger.error("处理数据["+line+"]异常:",e);
					continue;
				}
			}
			br.close();
			//将剩余数据导入到mysql
			doExport(list);
			list.clear();
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
						//备份到 backup目录下
						String backupName = StringUtils.replace(inputFile, ".f", ".COMPLETED");
						String backFilePath = "";
						if(backupPath.endsWith("/")){
							backFilePath = backupPath+backupName;
						}else{
							backFilePath = backupPath+File.separator+backupName;
						}
						File backFile =  new File(backFilePath);
						boolean flag = input.renameTo(backFile);
						logger.info("备份文件["+inputFile+"]到["+backFile+"]下文件名为["+backupName+"]完成，flag="+flag);
					}
				}
				
			
			} catch (Exception e) {
				logger.info("流关闭异常", e);
			}
		}
		
		return result;
	}
	
	private  String parseDataToSql(String tableName,String string){
		final StringBuffer insertSql = new StringBuffer();	
		final StringBuffer titleBuffer = new StringBuffer();
		final StringBuffer valueBuffer = new StringBuffer();
		try {
			String[] arr = string.replaceAll("\"","").split("\\&");
			
			for(int i=0;i<arr.length;i++){
				String str = arr[i];
				if(str.contains("HTTP")){
					str = str.split("\\s+")[0];
				}else if(str.contains("GET ")){
					str = str.split("\\?")[1];
				}
				String[] kv = str.split("\\=");
				String k = "";
				String v = "";
				if(kv.length>1){
					 k = kv[0];
					 v = kv[1];
				}else{
					//遇到值为空情况
					 k = kv[0];
					 v = "";
				}
				//拼接字段
				if(i>0){
					titleBuffer.append(","+k);
					valueBuffer.append(",'"+v+"'");
				}else{
					titleBuffer.append(k);
					valueBuffer.append("'"+v+"'");
				}
			}
			
			//组织sql
			insertSql.append("insert into "+tableName+"("+titleBuffer.toString()+") values(");
			insertSql.append(valueBuffer.toString());
			insertSql.append(")");
			
			
			return insertSql.toString();
			
		} catch (Exception e) {
			logger.error("解析串表["+tableName+"]对应串["+string+"]出错：",e);
			return "";
		}
	}
	
	/**
	 * 执行导入操作
	 * @param list
	 * @param targetTable
	 * @return
	 */
	public boolean doExport(List<String> list) {
		// 导出处理
		if (null!=list && list.size() > 0) {
			logger.info("start to insert to table");
			// exceptionRow =
			// DbUtil.executeUpdate(list,oracleTable,date,oracleEnv);
			// 分批导出
			String[] expSqls = new String[list.size()];
			list.toArray(expSqls);
			try {
				int[] resutl = adSupersspJt.batchUpdate(expSqls);
				logger.info("exec insert to table  numbers = " + resutl.length);
			} catch (Exception e) {
				logger.error("exec insert to table  fail : ", e);
			}
			logger.info("exec insert to table  end!");
		} else {
			logger.error("no data to insert to table!");
			return false;
		}
		return true;
	}

	public int getBachSize() {
		return bachSize;
	}

	public void setBachSize(int bachSize) {
		this.bachSize = bachSize;
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
