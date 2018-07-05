package com.voole.ad.export;



import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.voole.ad.main.IJobLife;
import com.voole.ad.send.SendAliyunService;

/**
 * 将hive表中数据到Mysql表中
 * @author shaoyl
 *
 */
public abstract class AbastractBaseExportJob implements IJobLife {
	protected static Logger logger = Logger.getLogger(AbastractBaseExportJob.class);
	protected Map<String, String> exportTaskMap;
	@Autowired
	public JdbcTemplate hiveJt;
	@Autowired
	public JdbcTemplate adSupersspJt;
	@Resource
	private SendAliyunService sendAliyunService;
	
	//导出每批次数量
	protected int bachSize = 100;
	//是否同步到阿里云(注：现在不采用此方式同步，使用RESTFul同步数据方式)
	protected int isSyncAliyun = 0;
	
	/**
	 * 执行job 中 hive jdbc的内容
	 * @param date
	 */
	@Override
	public void process(String time){
		
		logger.info("---------开始执行导出到Mysql任务-----------");
		long start = System.currentTimeMillis();
		int count = 0;
		
		//执行hive
		for (Iterator iterator = exportTaskMap.keySet().iterator(); iterator.hasNext();) {           

	        String key = (String) iterator.next();  //配置Oracle表名
	        String value  = exportTaskMap.get(key);//查询hive数据sql
	        
			Long startTime = System.currentTimeMillis();
			
			value = value.replaceAll("@yestoday", time);
			
			logger.info("开始执行导入表【"+key+"】日期为【"+time+"】的数据......");
			
			boolean result =  doSelect(value,key,time);
						
			logger.info("hivejob : 【"+time+"%"+key+"】 执行结果为："+result );
			count++;
			
			Long endTime = System.currentTimeMillis();
			
			logger.info("导入表【"+key+"】日期为【"+time+"】的数据完成，耗时 : ["+(endTime-startTime)/1000 +"] 秒!" );
	    }
		
	   long end = System.currentTimeMillis();
		
		logger.info("-------------------导出到Mysql任务【"+count+"】条执行完成，总耗时【"+(end-start)+"】毫秒！-------------------");
	
	}
	
	/**
	 * hive 导到 oracle 操作
	 * @param arg
	 */
	public boolean doSelect(String hiveSql,String toTable,String date){
		logger.info("start export data from hive to mysql :"+date);
		logger.info("hive sql is :"+ hiveSql);
		logger.info("to table is :"+ toTable);
		//删除mysql里指定日期数据
		boolean delFlag = doDelete(toTable, date);
		logger.info("doDelete talbe :["+toTable+"] flag = "+ delFlag);
		
		final String targetTable = toTable;
		//hive里查出表的数据
		final List<String> list = new ArrayList<String>();
		final StringBuffer insertSql = new StringBuffer();	
		final StringBuffer titleBuffer = new StringBuffer();
		final StringBuffer valueBuffer = new StringBuffer();
		//记录需要同步到阿里云数据
		final List<String> paramList = new ArrayList<String>();
		final StringBuffer paramBuffer = new StringBuffer();	
		
		//行数
		final AtomicLong at=new AtomicLong(0l);
		try {
			this.hiveJt.query(hiveSql, new RowMapper<Object>(){
				@Override
				public Object mapRow(ResultSet hiveRes, int rownum) throws SQLException {
//					System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)+","+rs.getString(4)+","+rs.getString(5)+","+rs.getString(6));
					ResultSetMetaData metaData = hiveRes.getMetaData();
					int columnCount = metaData.getColumnCount();
					
//					logger.info("rownum=="+rownum);
					if(isSyncAliyun==1){
						paramBuffer.append("tb=").append(targetTable);
					}
				
				
						for(int j = 1 ; j <= columnCount ; j ++ ){
							//处理字段名称
//							if(at.get()==0){
								String colName = metaData.getColumnName(j);
								if(colName.indexOf(".")>-1){
									colName = colName.substring(colName.indexOf(".")+1);
								}
								if(j>1){
									titleBuffer.append(","+colName);
								}else{
									titleBuffer.append(colName);
								}
//							}
							//处理数据
							String value = hiveRes.getString(j);
							String newValue = "";
							if(StringUtils.isBlank(value)){
								newValue="NULL";
							}else{
								newValue = "'"+value+"'";
							}
							//处理数据
							if(j > 1 ){
								valueBuffer.append(","+newValue);
							}
							else{
								valueBuffer.append(newValue);
							}
							//拼接发送阿里云串
							if(isSyncAliyun==1){
								paramBuffer.append("&").append(colName).append("=").append(value);
							}
						}
						
						//组织sql
						insertSql.append("insert into "+targetTable+"("+titleBuffer.toString()+") values(");
						insertSql.append(valueBuffer.toString());
						insertSql.append(")");
						list.add(insertSql.toString());
						
						if(isSyncAliyun==1){
							paramList.add(paramBuffer.toString());
							paramBuffer.setLength(0);
							if(at.get()==0){
								logger.info(" do send sql is  : ["+paramBuffer.toString()+"]");	
							}
						}
						
						if(at.get()==0){
							logger.info(" do export sql is  : ["+insertSql.toString()+"]");		
						}
						//置空拼接串
						titleBuffer.setLength(0);
						valueBuffer.setLength(0);
						insertSql.setLength(0);
						
						
						// do export
						if(at.incrementAndGet()%bachSize==0){
							logger.info(" get export datas : "+at.get());							
							boolean expFlag = doExport(list,targetTable);
							logger.info(" doExport end,expFlag=["+expFlag+"]");
							list.clear();
							
							if(isSyncAliyun==1){
								sendAliyunService.doSend(paramList);
								paramList.clear();
							}
							
						}
					
					return null;
				}				
			});
			//将剩余数据导入到mysql
			boolean expFlag = doExport(list,targetTable);
			logger.info(" all doExport end,expFlag=["+expFlag+"]");
			list.clear();
			
			//剩余数据发送阿里云
			if(isSyncAliyun==1){
				sendAliyunService.doSend(paramList);
				paramList.clear();
			}
			
		}
		catch(Exception e){
		   logger.error("查询hive数据出错：",e);
			return false;
		}
		
		logger.info(" get whole export datas : "+at.get());
		
		return true;
	}
	/**
	 * 执行删除操作
	 * @param targetTable
	 * @param daytime
	 * @return
	 */
	public boolean doDelete(String targetTable,String daytime){
		//导入前删除指定日期数据
		String delSql = "DELETE FROM "+targetTable+" WHERE daytime = "+daytime;
		try {
			 adSupersspJt.update(delSql);
			 logger.info("exec delSql = ["+delSql+"] end!");
			} catch (Exception e) {
				logger.error("exec delSql = ["+delSql+"]  fail : ",e);
				return false;
			}
		return true;
	}
	/**
	 * 执行导入操作
	 * @param list
	 * @param targetTable
	 * @return
	 */
	public boolean doExport(List<String> list, String targetTable) {
		// 导出处理
		if (list.size() > 0) {
			logger.info("start to export to table [" + targetTable + "]");
			// exceptionRow =
			// DbUtil.executeUpdate(list,oracleTable,date,oracleEnv);
			// 分批导出
			String[] expSqls = new String[list.size()];
			list.toArray(expSqls);
			try {
				int[] resutl = adSupersspJt.batchUpdate(expSqls);
				logger.info("exec export to table [" + targetTable	+ "] numbers = " + resutl.length);
			} catch (Exception e) {
				logger.error("exec export to table [" + targetTable	+ "]  fail : ", e);
			}
			logger.info("exec export to table [" + targetTable + "]  end!");
		} else {
			logger.error("no data to export to table [" + targetTable + "]!");
			return false;
		}
		logger.info("----- success load to table [" + targetTable
				+ "] ------- ");
		return true;
	}


	public Map<String, String> getExportTaskMap() {
		return exportTaskMap;
	}


	public void setExportTaskMap(Map<String, String> exportTaskMap) {
		this.exportTaskMap = exportTaskMap;
	}

	public int getBachSize() {
		return bachSize;
	}

	public void setBachSize(int bachSize) {
		this.bachSize = bachSize;
	}

	public int getIsSyncAliyun() {
		return isSyncAliyun;
	}

	public void setIsSyncAliyun(int isSyncAliyun) {
		this.isSyncAliyun = isSyncAliyun;
	}
	
}
