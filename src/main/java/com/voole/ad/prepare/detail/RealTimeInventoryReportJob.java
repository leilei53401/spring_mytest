package com.voole.ad.prepare.detail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.fastjson.JSON;
import com.voole.ad.model.inventory.Inventory;
import com.voole.ad.model.inventory.InventoryOperation;
import com.voole.ad.model.inventory.Location;
import com.voole.ad.prepare.AbstractPrepareJob;
import com.voole.ad.utils.HttpClientUtil;

/**
 * innerssp 上报实时库存。
 * @author shaoyl 20170505
 */
public class RealTimeInventoryReportJob extends AbstractPrepareJob{
	
	private String preSql;//查询需要上报的库存数据
	private String realtimeinventoryid;
	private String super_ssp_url;  //上报库存url
	
	@Autowired
	public JdbcTemplate adSupersspJt;
	

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(String time) {
		logger.info("--------------进入排期内有效节目预处理-------------------------");
		long start  = System.currentTimeMillis();
		DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMddHH");  
        DateTime dateTime = DateTime.parse(time, format);  
//        String dayTime = dateTime.toString("yyyy-MM-dd");
//		preSql = preSql.replaceAll("@time", dayTime);
        String newSql = preSql.replaceAll("@hourtime", time);
		logger.info("preSql 替换后为：" + newSql);
		List<Map<String,Object>> invList =  adSupersspJt.queryForList(newSql);
		//stamp	mediaid	adptype	adpid	te	playnum	devnum	last_update
		//2017-05-07 08:00:00	900100	90000099	15101010	0	220645	15710	2017-05-07 08:58:49
		Map<String, Object> formatDataMap = null;
		//按媒体，按媒体方广告位组织数据
		if(null!=invList && invList.size()>0){
			logger.info("获取到库存数据【"+invList.size()+"】条!");
			formatDataMap = formatData(invList);
		}else{
			logger.warn("未获取到库存数据!");
		}
	
		long end  = System.currentTimeMillis();
		
		logger.info("--------------查询库存数据并组织数据完成,耗时["+(end-start)+"]毫秒!------------------------");
		
		logger.info("-------------开始上报库存数据------------------------");
		
		
		if(null!=formatDataMap && formatDataMap.size()>0){
			logger.info("重新组织数据媒体【"+formatDataMap.size()+"】条!");
			
			//处理上报库存截止时间
	        //注：时间戳要设置在截止到当前小时(如 5点报 4点到5点的数据，截止时间为5点)
			//跨天时，00点的数据，截止时间为前一天的 23:59:59
			
			DateTime endDateTime = dateTime.plusHours(1);
		    long endInvTime = dateTime.plusHours(1).getMillis();
			if(endDateTime.toString("HH").equals("00")){
				endInvTime = endInvTime - 1000;
			}
			
			Iterator<String> mediaIt =   formatDataMap.keySet().iterator();
			while(mediaIt.hasNext()){
				String mediaAndOemId = mediaIt.next();
				String[] mediaOemArray =  mediaAndOemId.split("_");
				if(mediaOemArray.length<2){
					logger.warn("解析mediaid和oemid异常：mediaAndOemId=[" + mediaAndOemId+"]");
					continue;
				}
				String mediaId = mediaOemArray[0];
				String oemId = mediaOemArray[1];
				
				logger.info("解析后：mediaId = ["+mediaId+"] ,oemId=["+oemId+"]");
				
				Map<String,Object> adpIdMap =  (Map<String,Object>)formatDataMap.get(mediaAndOemId);
				
				logger.info("重新组织数据媒体["+mediaId+"],oemid=["+oemId+"] 下广告位【"+adpIdMap.size()+"】条!");
				//媒体方广告位
				Iterator<String> adpidIt = adpIdMap.keySet().iterator();
				while(adpidIt.hasNext()){
					String adpid = "";
					try {
						adpid = adpidIt.next();
						List<Map<String,Object>>  datas = (List<Map<String,Object>> )adpIdMap.get(adpid);
						//循环组织区域数据
						List<Location> locations = new ArrayList<Location>();
				
						for(Map<String,Object> data:datas){
							String areaCode = data.get("cityid").toString();
							String playnum = (null!=data.get("playnum"))?data.get("playnum").toString():"0";
							
							Location location =  new Location();
							location.setLocatid(areaCode);
							location.setQuantity(Integer.parseInt(playnum));
							locations.add(location);
						}
						
						 logger.info("实时库存上报mediaId=["+mediaId+"],oemid=["+oemId+"],adpid=["+adpid+"],locations:"+locations.size());
						 
						   if (locations.size() != 0) {
						    	DateTime currTime = new DateTime();
						    	String stamp = currTime.toString("yyyyMMddHHmmss");
						    	String optId = realtimeinventoryid+stamp;
						    	InventoryOperation inventoryOperation = new InventoryOperation();
								inventoryOperation.setOperation("PUT");
								inventoryOperation.setOperationId(optId);
								Inventory inventory =  new Inventory();
								inventory.setStamp(endInvTime+"");//计算库存截止时间
								inventory.setLocations(locations);
								inventoryOperation.setRealtimeinventory(inventory);
								
								//上报
								String url = super_ssp_url;
								url = url+"/"+mediaId+"/adp/"+adpid+"/realtimeinventory/"+ optId + "/oemid/" + oemId;
								
								logger.info("实时库存mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]上报URL:"+url);
								
								String json = JSON.toJSONString(inventoryOperation);
								
								logger.info("实时库存mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]上报json:"+json);
								String res = HttpClientUtil.httpJsonPost(url, json);
								if (StringUtils.isNotBlank(res)) {
									JSONObject jsonObject = JSONObject.fromObject(res);
									if ("0".equals(jsonObject.get("code").toString())) {
										logger.info("mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]上报实时库存成功");
									} else {
										logger.info("mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]上报实时库存失败："+jsonObject.get("message"));
									}
								}else{
									logger.warn("mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]上报库存返回为空！");
								}
						    } else {
						    	logger.warn("没有找到mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]实时库存信息");
						    }
						    
							logger.info("------------结束mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]上报实时库存------------------------");
					} catch (NumberFormatException e) {
						logger.error("数字转化异常!",e);
					} catch (Exception e) {
						logger.error("上报mediaid["+mediaId+"],oemid=["+oemId+"],adpid["+adpid+"]库存异常：",e);
					}
				}
			}
		}else{
			logger.warn("未获取到要上报的库存数据");
		}
		
		long endInvTime = System.currentTimeMillis();
		logger.info("-------------结束上报库存数据,耗时("+(endInvTime-end)/1000+")秒！------------------------");
		
		
	}

	/**
	 * 组织数据
	 * @param invList
	 * @return
	 */
	private Map<String, Object> formatData(List<Map<String, Object>> invList) {
		Map<String, Object> formatData = new HashMap<String,Object>();			
		for(Map<String,Object> data:invList){
		  String mediaId = (null!=data.get("mediaid"))?data.get("mediaid").toString():"";
		  String oemid = (null!=data.get("oemid"))?data.get("oemid").toString():"";
		  String adpid = (null!=data.get("adpid"))?data.get("adpid").toString():"";
		  
		  String key = mediaId+"_"+oemid;
		  
		  if(null!=formatData.get(key)){
			  Map<String,Object> adpidMap = (Map<String,Object>)formatData.get(key);
			  if(null!=adpidMap.get(adpid)){
				  List<Map<String,Object>> adpidList = (List<Map<String,Object>>)adpidMap.get(adpid);
				  adpidList.add(data);
			  }else{
				  List<Map<String,Object>> adpidList = new  ArrayList<Map<String,Object>> ();
				  adpidList.add(data);
				 
				  adpidMap.put(adpid,adpidList);
			  }
		  }else{
			  List<Map<String,Object>> adpidList = new  ArrayList<Map<String,Object>> ();
			  adpidList.add(data);
			  Map<String,Object> adpidMap  =  new HashMap<String,Object>();
			  adpidMap.put(adpid,adpidList);			  
			  formatData.put(key, adpidMap);
		  }
		}
		return formatData;
	}
	
	

	public String getPreSql() {
		return preSql;
	}

	public void setPreSql(String preSql) {
		this.preSql = preSql;
	}
	
	

	public String getRealtimeinventoryid() {
		return realtimeinventoryid;
	}

	public void setRealtimeinventoryid(String realtimeinventoryid) {
		this.realtimeinventoryid = realtimeinventoryid;
	}
	

	public String getSuper_ssp_url() {
		return super_ssp_url;
	}

	public void setSuper_ssp_url(String super_ssp_url) {
		this.super_ssp_url = super_ssp_url;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}
	
	

}
