package com.voole.ad.preplan.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class CombMacServiceImp implements CombMacService {
	
	Logger logger = Logger.getLogger(CombMacServiceImp.class);
	private String unionMacComb;

	private String insertMacComb;

	private String insertMacPrePlan;
	
	private String selectMacComb;
	
	private String truncateMac;
	
	private String rooturl;
	
	private String addPatition;
	
	@Autowired
	public JdbcTemplate hiveJt;

	@Override
	public List<String> selectMacPartition() {
		// 查询需要合并的表 tmp_preplan_mac_status 和 preplan_mac_status 的所有分区
		String showPartitionTmp = "show partitions tmp_preplan_mac_status";
		String showPartitionPre = "show partitions preplan_mac_status";
		List<Map<String, Object>> tmpList = hiveJt.queryForList(showPartitionTmp);
		List<Map<String, Object>> preList = hiveJt.queryForList(showPartitionPre);
		List<String> rslist = new ArrayList<String>();
		List<String> unionPartitions = new ArrayList<String>();
		if (tmpList != null && !tmpList.isEmpty()) {
			for (int i = 0; i < tmpList.size(); i++) {
				Map<String, Object> tmpMap = tmpList.get(i);
				String tmpPartition = MapUtils.getString(tmpMap, "partition");
				unionPartitions.add(tmpPartition);
			}
			if (preList != null && !preList.isEmpty()) {
				for (int j = 0; j < preList.size(); j++) {
					Map<String, Object> preMap = preList.get(j);
					String prePartition = MapUtils.getString(preMap, "partition");
					unionPartitions.add(prePartition);
				}
			}
		} else {
			if (preList != null && !preList.isEmpty()) {
				for (int i = 0; i < preList.size(); i++) {
					Map<String, Object> preMap = preList.get(i);
					String prePartition = MapUtils.getString(preMap, "partition");
					unionPartitions.add(prePartition);
				}
			} else {
				logger.info(
						"---------------------表tmp_preplan_mac_status和表preplan_mac_status分区为空---------------------");
			}
		}
		if (unionPartitions != null && !unionPartitions.isEmpty()) {
			Set<String> set = new HashSet<String>();
			set.addAll(unionPartitions);
			rslist.addAll(set);
		}
		return rslist;
	}

	@Override
	public boolean combMacTable(String daytime, String planid) {
		//按查询到的分区，合并tmp_preplan_mac_status 和 preplan_mac_status 数据到 comb_preplan_mac_status
		boolean ok = false;
		String combTableSQL = null;
		String combTable = "from (" + unionMacComb.trim() + ") a" ;
		String insertSql = insertMacComb.replace("@daytime", daytime).replace("@planid", planid);
		String selectSql = selectMacComb.replace("@daytime", daytime).replace("@planid", planid);
		combTableSQL = combTable + insertSql + selectSql;
		hiveJt.execute(combTableSQL);
		ok = true;
		/*List<String> parList = new ArrayList<String>(); 
		//parList = selectMacPartition();
		if (parList != null && !parList.isEmpty()) {
		//	StringBuffer tmpcombTable = new StringBuffer();
			for (int i = 0; i < parList.size(); i++) {
				String partitions = parList.get(i);
				String[] partitionArr = partitions.split("/");
				String daytime = (partitionArr[0].split("="))[1];
				String planid = (partitionArr[1].split("="))[1];
				String[] insertMacComba = new String[parList.size()];
				String[] selectMacComba = new String[parList.size()];
				insertMacComba[i] = insertMacComb.replace("@daytime", daytime).replace("@planid", planid);
				selectMacComba[i] = selectMacComb.replace("@daytime", daytime).replace("@planid", planid);
				tmpcombTable.append(insertMacComba[i]).append(selectMacComba[i]);
			}
			combTableSQL = combTable + " " + tmpcombTable.toString();
		}*/
		return ok;
	}

	@Override
	public List<String> selectMacPrePlanPartition() {
		//查询需要将comb_preplan_mac_status 导入到  preplan_mac_status 的所有分区
		String selectPrePlanPartition = "show partitions startpre.comb_preplan_mac_status";
		List<Map<String, Object>> partList = hiveJt.queryForList(selectPrePlanPartition);
		List<String> rsList = new ArrayList<String>();
		if (partList != null && !partList.isEmpty()) {
			for (int i = 0; i < partList.size(); i++) {
				Map<String, Object> rsMap = partList.get(i); 
				String partitions = MapUtils.getString(rsMap, "partition");
				rsList.add(partitions);
			}
		}
		return rsList;
	}

	@Override
	public boolean combMacToPrePlan(String daytime, String planid) {
		// 按查询到的分区，将comb_preplan_mac_status的数据导入 preplan_mac_status
		boolean ok = false;
		String insertPrePlanTable = "from startpre.comb_preplan_mac_status a";
		String insertSql = insertMacPrePlan.replace("@daytime", daytime).replace("@planid", planid);
		String selectSql = selectMacComb.replace("@daytime", daytime).replace("@planid", planid);
		String insertPrePlanTableSQL = insertPrePlanTable + insertSql + selectSql;
		hiveJt.execute(insertPrePlanTableSQL);
		ok = true;
		/*List<String> partList = selectMacPrePlanPartition();
		if (partList != null && !partList.isEmpty()) {
			StringBuffer tmpInsertPreTable = new StringBuffer();
			for (int i = 0; i < partList.size(); i++) {
				String partitionsStr = partList.get(i);
				String[] insertMacPrePlan1 = new String[partList.size()];
				String[] selectMacPrePlan1 = new String[partList.size()];
				String[] partitions = partitionsStr.split("/");
				String daytime = (partitions[0].split("="))[1];
				String planid = (partitions[1].split("="))[1];
				insertMacPrePlan1[i] = insertMacPrePlan.replace("@daytime", daytime).replace("@planid", planid);
				selectMacPrePlan1[i] = selectMacComb.replace("@daytime", daytime).replace("@planid", planid);
				tmpInsertPreTable.append(insertMacPrePlan1[i]).append(selectMacPrePlan1[i]);
			}
			String insertPrePlanTableSQL = insertPrePlanTable + " " + tmpInsertPreTable.toString();
			hiveJt.execute(insertPrePlanTableSQL);
			ok = true;
		}*/
		return ok;
	}

	
	@Override
	public  String httpJsonPost(String planid,String operate,String jsonString) throws IOException{
		String result="";
		int socketTimeout = 120000;
		int connectTimeout = 30000;
		String url = rooturl + planid + "/" + "operate" + "/" + operate;
		CloseableHttpClient  client  = HttpClientBuilder.create().build();
		try{
			HttpPost httpost = new HttpPost(url.trim());
			httpost.setHeader("Content-Type","application/json;charset=UTF-8");
			HttpEntity  entity  = new StringEntity(jsonString,"utf-8");
			httpost.setEntity(entity);
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout).build();//设置请求和传输超时时间  
			httpost.setConfig(requestConfig);
			HttpResponse response  = client.execute(httpost);
			int code = response.getStatusLine().getStatusCode();
			logger.debug("response code:"+ code);
			HttpEntity responseEntity = response.getEntity(); 
			result = EntityUtils.toString(responseEntity,"utf-8");
			client.close();
		} catch (Exception e){
			logger.error("请求["+url+"]出现异常：",e);
		}finally{
			if(client!=null){
				client.close();
			}
			logger.info("结束请求["+url+"]");
		}
		return result;
	}
	
	@Override
	public void truncateTable() {
		// 清空表:startpre.tmp_preplan_mac_status内的数据
		String truncateTableSQL = truncateMac.replace("@tablename", "startpre.tmp_preplan_mac_status");
		hiveJt.execute(truncateTableSQL);
	}
	
	@Override
	public void addTablePartition(String table, String daytime, String planid) {
		String addTP = addPatition.replace("@tablename", table);
		String addTPD = addTP.replace("@daytime", daytime);
		String addTPDP = addTPD.replace("@planid", planid);
		hiveJt.execute(addTPDP);
	}
	
	public String getUnionMacComb() {
		return unionMacComb;
	}

	public void setUnionMacComb(String unionMacComb) {
		this.unionMacComb = unionMacComb;
	}

	public String getInsertMacComb() {
		return insertMacComb;
	}

	public void setInsertMacComb(String insertMacComb) {
		this.insertMacComb = insertMacComb;
	}

	public String getInsertMacPrePlan() {
		return insertMacPrePlan;
	}

	public void setInsertMacPrePlan(String insertMacPrePlan) {
		this.insertMacPrePlan = insertMacPrePlan;
	}

	public String getSelectMacComb() {
		return selectMacComb;
	}

	public void setSelectMacComb(String selectMacComb) {
		this.selectMacComb = selectMacComb;
	}

	public String getTruncateMac() {
		return truncateMac;
	}

	public void setTruncateMac(String truncateMac) {
		this.truncateMac = truncateMac;
	}

	public String getRooturl() {
		return rooturl;
	}

	public void setRooturl(String rooturl) {
		this.rooturl = rooturl;
	}

	public String getAddPatition() {
		return addPatition;
	}

	public void setAddPatition(String addPatition) {
		this.addPatition = addPatition;
	}

}
