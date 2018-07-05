package com.voole.ad.prepare.detail;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import com.voole.ad.file.AbastractBaseFileJob;
import com.voole.ad.prepare.AbstractPrepareJob;
import com.voole.ad.utils.HttpClientUtil;

/**
 * 从阿里云同步字典表数据到机房
 * @author shaoyl
 * 每小时更新一次
 */
public  class SyncDictDbJob extends AbstractPrepareJob {
	protected static Logger logger = Logger.getLogger(AbastractBaseFileJob.class);

	@Autowired
	public JdbcTemplate adSupersspJt;
	//RESTful url接口
	private String urlPrefix;
	private  String tables;//要同步的表
	private Map<String,String> syncFieldsMap;//配置每个表要同步的指定字段
	private Map<String,String> skipStampValueMap;//忽略的字段
	private Map<String,String> replaceStampValueMap;//要替换时间函数的字段
		
	/**
	 * 执行job 中 hive jdbc的内容
	 * @param date
	 */
	@Override
	public void process(String time){
		logger.info("------------time=["+time+"]--开始同步字典表数据 tables["+tables+"]-------------------------");
		
		String[] tableArray = tables.split(",");
		
		for(String table:tableArray){
			logger.info("------------start sync talbe ["+table+"]-------------------------");
			String url = urlPrefix + table;
			logger.debug("url=" + url);
			
			JSONArray array = null;
			try {
				 array = new JSONArray();
		
				String res = HttpClientUtil.httpGet(url,60000,60000);
				if (StringUtils.isNotBlank(res)) {
					array = JSONArray.fromObject(res);
					logger.debug("返回数据===" + array.toString());
				}else{
					logger.warn("sync table ["+table+"] 返回为空！");
				}
				
			} catch (ClientProtocolException e) {
				logger.error("sync table ["+table+"] error:",e);
				return;
			} catch (IOException e) {
				logger.error("sync table ["+table+"] error:",e);
				return;
			} catch (Exception e) {
				logger.error("sync table ["+table+"] error:",e);
				return;
			}  
			
			//执行入库操作
			if(null==array || array.size()==0){
				logger.error("未获取到table=["+table+"]的数据！");
				return;
			}else{
				logger.info("获取到table=["+table+"]的数据【"+array.size()+"】条！");
			}		
			
			parseData2DB(table,array);
			
			logger.info("------------end sync talbe ["+table+"]-------------------------");
		}//for table  end
		
		logger.info("------------结束time=["+time+"]同步字典表数据任务--------------------");
	}
	
	
	public void parseData2DB(String tableName,JSONArray array) {
			
		List<String> list = new ArrayList<String>();
		final StringBuffer insertSql = new StringBuffer();	
		final StringBuffer titleBuffer = new StringBuffer();
		final StringBuffer valueBuffer = new StringBuffer();
		//需要同步的字段
		String syncFields = syncFieldsMap.get(tableName);
		String [] syncFieldsArray = null;
		List<String> syncFieldsList = new ArrayList<String>();
		if(StringUtils.isNotBlank(syncFields)){
			syncFieldsArray = syncFields.split(",");
			syncFieldsList = Arrays.asList(syncFieldsArray);
		}
		logger.debug("table : ["+tableName+"] 需要同步的字段为： "+syncFieldsList.toString());
		//忽略的字段
		String skipWords = skipStampValueMap.get(tableName);
		String [] skipWordsArray = null;
		if(StringUtils.isNotBlank(skipWords)){
			skipWordsArray = skipWords.split(",");
		}
	
		//替换时间函数的字段
		String replaceWords = replaceStampValueMap.get(tableName);
		String [] replaceWordsArray =  null;
		if(StringUtils.isNotBlank(replaceWords)){
			 replaceWordsArray = replaceWords.split(",");
		}
	
		for(int i=0;i<array.size();i++){
			JSONObject obj =  array.getJSONObject(i);
			Iterator<String> keys = obj.keys();
			int j=0;
			while(keys.hasNext()){
				String title = keys.next();
				
				if(!syncFieldsList.contains(title)){
					continue;//数据字段不在同步字段列表中不拼接sql
				}
				
				String value = obj.get(title).toString();
				
				if(StringUtils.isBlank(value)||"null".equalsIgnoreCase(value)){
//					value = "";
					continue;
				}
				
				//忽略字段(如：自动生成时间的字段)
			/*	if("stamp".equalsIgnoreCase(title)){
					continue;
				}*/
				boolean skip=false;
				if(null!=skipWordsArray){
					for(String skipWord:skipWordsArray){
						if(title.equalsIgnoreCase(skipWord)){
							skip=true;
							break;
						}
					}
				}
				
				if(skip) continue;
				
				//替换时间戳字段
				boolean replace=false;
				if(null!=replaceWordsArray){
					for(String replaceWord:replaceWordsArray){
						if(title.equalsIgnoreCase(replaceWord)){
							replace=true;
							break;
						}
					}
				}
				
				if(replace){
					String v = StringUtils.substring(value, 0, 10);
					value = "from_unixtime("+v+")"; 
				}
				
				//拼接字段
				if(j > 0){
					titleBuffer.append(","+title);
					if(replace){
						valueBuffer.append(","+value);
					}else{
						valueBuffer.append(",'"+value+"'");
					}
				}else{
					titleBuffer.append(title);
					if(replace){
						valueBuffer.append(value);
					}else{
						valueBuffer.append("'"+value+"'");
					}
				}
				
				j++;
				
			}
			//组织 insert sql;
			insertSql.append("insert into "+tableName+"("+titleBuffer.toString()+") values(");
			insertSql.append(valueBuffer.toString());
			insertSql.append(")");
			
			logger.debug("insertSql=["+insertSql.toString()+"]");
			
			 list.add(insertSql.toString());
			 
			/*if(at.incrementAndGet()%bachSize==0){
				doExport(list);
				list.clear();
			}*/
				
			//置空拼接串
			titleBuffer.setLength(0);
			valueBuffer.setLength(0);
			insertSql.setLength(0);
			
		}
		//执行导入
		if(list!=null && list.size()>0){
			doExport(tableName,list);
			list.clear();
		}else{
			logger.info("table : ["+tableName+"], list == null");
		}
	}
	
	
	
	/**
	 * 执行导入操作
	 * @param list
	 * @param targetTable
	 * @return
	 */
	@Transactional
	public boolean doExport(String tableName,List<String> list) {
		logger.info("start to del table ["+tableName+"] data");
		int count = adSupersspJt.update("delete from "+tableName);
		logger.info("end del table ["+tableName+"] data,del data rows = " +count);
		// 导出处理
		if (count>=0 && null!=list && list.size() > 0) {
			logger.info("start to insert to table");
			// exceptionRow =
			// DbUtil.executeUpdate(list,oracleTable,date,oracleEnv);
			// 分批导出
			String[] expSqls = new String[list.size()];
			list.toArray(expSqls);
			try {
				int[] resutl = adSupersspJt.batchUpdate(expSqls);
				logger.info("exec insert to table ["+tableName+"] numbers = " + resutl.length);
			} catch (Exception e) {
				logger.error("exec insert to table ["+tableName+"] fail : ", e);
			}
			logger.info("exec insert to table ["+tableName+"] end!");
		} else {
			logger.error("no data to insert to table ["+tableName+"]!");
			return false;
		}
		return true;
	}
	
	
	
	public String getUrlPrefix() {
		return urlPrefix;
	}

	public void setUrlPrefix(String urlPrefix) {
		this.urlPrefix = urlPrefix;
	}

	
	public String getTables() {
		return tables;
	}


	public void setTables(String tables) {
		this.tables = tables;
	}
	

	public Map<String, String> getSyncFieldsMap() {
		return syncFieldsMap;
	}


	public void setSyncFieldsMap(Map<String, String> syncFieldsMap) {
		this.syncFieldsMap = syncFieldsMap;
	}


	public Map<String, String> getSkipStampValueMap() {
		return skipStampValueMap;
	}


	public void setSkipStampValueMap(Map<String, String> skipStampValueMap) {
		this.skipStampValueMap = skipStampValueMap;
	}


	public Map<String, String> getReplaceStampValueMap() {
		return replaceStampValueMap;
	}


	public void setReplaceStampValueMap(Map<String, String> replaceStampValueMap) {
		this.replaceStampValueMap = replaceStampValueMap;
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
