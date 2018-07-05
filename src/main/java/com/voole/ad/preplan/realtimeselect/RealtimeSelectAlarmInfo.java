package com.voole.ad.preplan.realtimeselect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.main.IJobLife;

public class RealtimeSelectAlarmInfo implements IJobLife{
	Logger looger = Logger.getLogger(RealtimeSelectAlarmInfo.class);
	
	private String selectAlarmAllInfo; //查询报警创意的媒体广告位
	
	private String selectAlarmPlan; //查找有效的符合开机实时广告的排期
	
	private String selectPrePlan;
	@Autowired
	public JdbcTemplate adSupersspJt;
	
	public List<Map<String, Object>> selectAlarmInfo() {
		/*
		 * 查询严重报警创意的媒体广告位等信息
		 * oemid,mediaid,mediaid,creativeid,areaCode,ad_position,diffnum,positionid,super_adposid
		 */
		List<Map<String, Object>> alarmList = adSupersspJt.queryForList(selectAlarmAllInfo);
		List<Map<String, Object>> rsList = new ArrayList<Map<String, Object>>();
		if (alarmList != null && !alarmList.isEmpty()) {
			for (int i = 0; i < alarmList.size(); i++) {
				Map<String, Object> alarmMap = alarmList.get(i);
				int creativeid = MapUtils.getIntValue(alarmMap, "creativeid");
				int diffnum = MapUtils.getIntValue(alarmMap, "diffnum");
				int areaCode = MapUtils.getIntValue(alarmMap, "areaCode");
				int mediaid = MapUtils.getIntValue(alarmMap, "mediaid");
				String postionid = MapUtils.getString(alarmMap, "positionid");
				String oemid = MapUtils.getString(alarmMap, "oemid");
				String sqlP = selectAlarmPlan.replace("@postionid", postionid);
				String sqlPO = sqlP.replace("@oemid", oemid);
				// 根据媒体广告位和渠道oemid 查询报警创意的媒体排期 planid,ad_location_code,oemid
				List<Map<String, Object>> alarmPlanList = adSupersspJt.queryForList(sqlPO);
				if (alarmPlanList != null && !alarmPlanList.isEmpty()) {
					//判断排期对应的预排期是否还在投放期内，是否为有效状态
					for (int j = 0; j < alarmPlanList.size(); j++) {
						Map<String, Object> alarmPlanMap = alarmPlanList.get(j);
						int alarmPlanid = MapUtils.getIntValue(alarmPlanMap, "planid");
						List<Map<String, Object>> prePlanList = adSupersspJt.queryForList(selectPrePlan);
						if (prePlanList != null && !prePlanList.isEmpty()) {
							for (int k = 0; k < prePlanList.size(); k++) {
								Map<String, Object> prePlanMap = prePlanList.get(k);
								Map<String, Object> rsMap = new HashMap<String, Object>();
								int prePlanid = MapUtils.getIntValue(prePlanMap, "planid");
								if (alarmPlanid == prePlanid) {
									rsMap.put("planid", alarmPlanid);
									rsMap.put("oemid", oemid);
									rsMap.put("mediaid", mediaid);
									rsMap.put("creativeid", creativeid);
									rsMap.put("areaCode", areaCode);
									rsMap.put("diffnum", diffnum);
									rsList.add(rsMap);
								} 
							}
						} else {
							looger.error("---------------------------请检查排期:" + alarmPlanid + "是否已生成预排期---------------------------");
						}
					}
				} else {
					looger.info("---------------------------创意:"+ creativeid + " 不是商投开机广告---------------------------");
				}
			}
		} else {
			looger.info("---------------------------无报警商投广告创意---------------------------");
		}
		return rsList;
	}
	
	
	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(String time) {
		selectAlarmInfo();
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	public void setAdSupersspJt(JdbcTemplate adSupersspJt) {
		this.adSupersspJt = adSupersspJt;
	}

	public void setSelectAlarmAllInfo(String selectAlarmAllInfo) {
		this.selectAlarmAllInfo = selectAlarmAllInfo;
	}

	public void setSelectAlarmPlan(String selectAlarmPlan) {
		this.selectAlarmPlan = selectAlarmPlan;
	}

	public void setSelectPrePlan(String selectPrePlan) {
		this.selectPrePlan = selectPrePlan;
	}
	
	
	
}
