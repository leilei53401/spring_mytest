package com.voole.ad.extopt.detail;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.fastjson.JSON;
import com.voole.ad.extopt.AbstractExtOptJob;
import com.voole.ad.utils.HttpClientUtil;

/**
 * 离线报表同步到阿里云数据库 v2 
 * 本地同步结束后，单独调用平台接口同步
 * 与本地同步任务分离开
 * @author shaoyl 20170505
 * @TODO:
 */
public class SyncReportDbExtJob extends AbstractExtOptJob{
	
	//RESTful url接口
    private String urlPrefix;
	private  String tables;//要同步的表
	
	@Autowired
	public JdbcTemplate adSupersspJt;
	

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(String time) {
		
		logger.info("------------time=["+time+"]--开始离线报表数据 tables["+tables+"]-------------------------");
		long startTime = System.currentTimeMillis();
		
		String[] tableArray = tables.split(",");
		
		for(String table:tableArray){
			logger.info("------------start sync talbe ["+table+"],and daytime=["+time+"]-------------------------");
			long start  = System.currentTimeMillis();
			//查询数据
			
			String sql  = "select * from "+table+" where daytime='"+time+"'";
			
			logger.info("get table=["+table+"] daytime=["+time+"] data sql is :"+sql);
			String rerportJson="";
			List<Map<String,Object>> reportList =  adSupersspJt.queryForList(sql);
			if(null!=reportList && reportList.size()>0){
				logger.info("获取到table=["+table+"] daytime=["+time+"]数据【"+reportList.size()+"】条!");
				
				rerportJson = JSON.toJSONString(reportList);
				
				String url = urlPrefix + table +"/daytime/"+time;
				
				logger.info("table=["+table+"] daytime=["+time+"]上报URL:"+url);
				
				logger.debug("table=["+table+"] daytime=["+time+"]上报json:"+rerportJson);
				
				String res = "";
				try {
					res = HttpClientUtil.httpJsonPost(url,rerportJson,60000,60000);
				} catch (ClientProtocolException e) {
					logger.error("table=["+table+"] daytime=["+time+"]同步数据发生异常：",e);
				} catch (IOException e) {
					logger.error("table=["+table+"] daytime=["+time+"]同步数据发生异常：",e);
				}
				if (StringUtils.isNotBlank(res)) {
					JSONObject jsonObject = JSONObject.fromObject(res);
					if ("0".equals(jsonObject.get("code").toString())) {
						logger.info("table=["+table+"] daytime=["+time+"]同步成功");
					} else {
						logger.info("table=["+table+"] daytime=["+time+"]同步失败："+jsonObject.get("message"));
					}
				}else{
					logger.warn("table=["+table+"] daytime=["+time+"]同步数据返回为空！");
				}
				
			}else{
				logger.warn("未获取到table=["+table+"] daytime=["+time+"]的数据!");
			}
			long stop  = System.currentTimeMillis();
			logger.info("------------stop sync talbe ["+table+"],and daytime=["+time+"]--耗时["+(stop-start)+"]毫秒-----------------------");
		
		}
		
		long endTime = System.currentTimeMillis();
		logger.info("-------------结束离线报表数据同步任务,耗时("+(endTime-startTime)/1000+")秒！------------------------");
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
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
	
	

}
