package com.voole.ad.preplan.realtimeselect;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.main.IJobLife;
import com.voole.ad.preplan.common.StoreMacRredis;
import com.voole.ad.preplan.common.UtilityClass;



public class RealtimeSelectMac implements IJobLife {

	Logger logger = Logger.getLogger(RealtimeSelectMac.class);
	private String refreshTable;
	
	private String selectPrePlanInfo;

	private String selectAddMacArea;

	private String selectDelMacArea;

	private String selectAreaProp;
	
	private RealtimeSelectAlarmInfo realtimeAlarm;

	private StoreMacRredis storeMacToRredis;
	
	@Autowired
	public JdbcTemplate adSupersspJt;

	@Autowired
	public JdbcTemplate hiveJt;
	
	@Autowired
	public JdbcTemplate impalaJt;

	/**
	 * 根据报警创意查询到的符合开机广告条件的排期进行终端调整，返回的结果包括
	 * planid,oemid,mediaid,creativeid,areacode,diffnum
	 * 
	 * @throws ParseException
	 */
	public void selectRealtimeMac() throws ParseException {
		
		List<Map<String, Object>> alarmInfoList = realtimeAlarm.selectAlarmInfo();
		Map<String, List<String>> rsmacMap = new HashMap<String, List<String>>();
		if (alarmInfoList != null && !alarmInfoList.isEmpty()) {
			for (int i = 0; i < alarmInfoList.size(); i++) {
				Map<String, Object> alarmMap = alarmInfoList.get(i);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
				int planid = MapUtils.getIntValue(alarmMap, "planid");
				int oemid = MapUtils.getIntValue(alarmMap, "oemid");
				int areaCode = MapUtils.getIntValue(alarmMap, "areaCode");
				int diffnum = MapUtils.getIntValue(alarmMap, "diffnum");
				/*
				 * 根据报警planid和oemid查询预排期详细信息
				 * planid,preplan_starttime,preplan_stoptime,areaflag,oemidflag,
				 * playtimesthreshold,playtimesflag,oemid,provinceid,cityid
				 */
				String sqlPrePlanP = selectPrePlanInfo.replace("@planid", String.valueOf(planid));
				String sqlPrePlanPO = sqlPrePlanP.replace("@oemid", String.valueOf(oemid));
				List<String> rsmacList = new ArrayList<String>();
				List<Map<String, Object>> prePlanList = adSupersspJt.queryForList(sqlPrePlanPO);
				if (prePlanList != null && !prePlanList.isEmpty()) {
					for (int j = 0; j < prePlanList.size(); j++) {
						Map<String, Object> prePlanMap = prePlanList.get(j);
					//	Date prePlanStarttime = sdf.parse(MapUtils.getString(prePlanMap, "preplan_starttime"));
						Date prePlanStoptime = sdf.parse(MapUtils.getString(prePlanMap, "preplan_stoptime"));
						if (areaCode == -1) {
							// 满足目前不支持具体区域报警情况
							int areaflag = MapUtils.getIntValue(prePlanMap, "areaflag");
							if (areaflag == 1) {
								// 区域限制
								String cityid = MapUtils.getString(prePlanMap, "cityid");
								List<Date> days = UtilityClass.findDates(new Date(), prePlanStoptime);
								for (int k = 0; k < days.size(); k++) {
									DateTime day = new DateTime(days.get(k));
									String tagid = String.valueOf(day.getDayOfWeek());
									String daytime = sdf1.format(day);
									// 该排期对应的投放区域的个数
									int areaCount = prePlanList.size();
									// 判断是新增还是删除终端操作
									if (diffnum < 0) {
										// 增终端----每个区域调整的量
										int addNum = (int) Math.rint(Math.abs(diffnum) * 1000 / areaCount);
										// 按媒体、oemid、市、tagid进行hive终端查询
										String sqlSelectMacD = selectAddMacArea.replace("@daytime", daytime);
										String sqlSelectMacDO = sqlSelectMacD.replace("@oemid", String.valueOf(oemid));
										String sqlSelectMacDOT = sqlSelectMacDO.replace("@tagid", tagid);
										String sqlSelectMacDOTC = sqlSelectMacDOT.replace("@cityid", cityid);
										String sqlSelectMacDOTCP = sqlSelectMacDOTC.replace("@planid", String.valueOf(planid));
										String replace = refreshTable.replace("@table", "startpre_mac_dict");
										impalaJt.execute(replace);
										List<Map<String, Object>> macDOTCList = impalaJt.queryForList(sqlSelectMacDOTCP);
										if (macDOTCList != null && !macDOTCList.isEmpty()) {
											double sum = 0.00;
											int count = 0;
											for (int l = 0; l < macDOTCList.size(); l++) {
												Map<String, Object> macMap = macDOTCList.get(l);
												double num = MapUtils.getDoubleValue(macMap, "num", 0.00);
												sum = sum + num;
												String mac = MapUtils.getString(macMap, "mac");
												rsmacList.add(mac);
												count++;
												if (sum >= addNum) {
													break;
												}
												if (count == macDOTCList.size()) {
													if (sum < addNum) {
														logger.info("----------------------区域限制情况下,渠道:" + oemid + "_城市:"
																+ cityid + "终端量不足,差" + (addNum - sum)
																+ "----------------------");
													}
												}
											}
											rsmacMap.put(String.valueOf(planid), rsmacList);
											// 存储到redis
											storeMacToRredis.storeAddMacToFile(rsmacMap, daytime, String.valueOf(planid));
											storeMacToRredis.storeMacToRedis(daytime, String.valueOf(planid), "add");
										} else {
											logger.info("------------------------区域限制情况下,渠道:" + oemid + "_城市:" + cityid
													+ "无符合要求的终端增加------------------------");
										}

									} else {
										// 删终端 ----每个区域调整的量
										int delNum = (int) Math.rint(diffnum * 1000 / areaCount);
										String sqlDMA = selectDelMacArea.replace("@daytime", daytime);
										String sqlDMAT = sqlDMA.replace("@tagid", tagid);
										String sqlDMATO = sqlDMAT.replace("@oemid", String.valueOf(oemid));
										String sqlDMATOC = sqlDMATO.replace("@cityid", cityid);
										String sqlDMATOCP = sqlDMATOC.replace("@planid", String.valueOf(planid));
										String replace = refreshTable.replace("@table", "startpre_mac_dict");
										impalaJt.execute(replace);
										List<Map<String, Object>> macDOTCList = impalaJt.queryForList(sqlDMATOCP);
										if (macDOTCList != null && !macDOTCList.isEmpty()) {
											double sum = 0.00;
											int count = 0;
											for (int l = 0; l < macDOTCList.size(); l++) {
												Map<String, Object> macMap = macDOTCList.get(l);
												double num = MapUtils.getDoubleValue(macMap, "num", 0.00);
												sum = sum + num;
												String mac = MapUtils.getString(macMap, "mac");
												rsmacList.add(mac);
												count++;
												if (sum >= delNum) {
													break;
												}
												if (count == macDOTCList.size()) {
													if (sum < delNum) {
														logger.info("----------------------区域限制情况下,渠道:" + oemid + "_城市:"
																+ cityid + "需要删除的终端量不足,差" + (delNum - sum)
																+ "----------------------");
													}
												}
											}
											rsmacMap.put(String.valueOf(planid), rsmacList);
											storeMacToRredis.storeDelMacToFile(rsmacMap, daytime, String.valueOf(planid));
											storeMacToRredis.storeMacToRedis(daytime, String.valueOf(planid), "del");
										} else {
											logger.info("------------------------区域限制情况下,渠道:" + oemid + "_城市:" + cityid
													+ "无符合要求的终端删除------------------------");
										}

									}
								}
							} else {
								// 区域不限
								List<Date> days = UtilityClass.findDates(new Date(), prePlanStoptime);
								if (days != null && !days.isEmpty()) {
									DateTime day = new DateTime(days.get(j).getTime());
									String daytime = sdf1.format(days.get(j));
									String tagid = String.valueOf(day.getDayOfWeek());
									String selectAreaProO = selectAreaProp.replace("@oemid", String.valueOf(oemid));
									// 查询媒体、oemid的区域占比
									List<Map<String, Object>> areaProList = adSupersspJt.queryForList(selectAreaProO);
									if (areaProList != null && !areaProList.isEmpty()) {
										for (int k = 0; k < areaProList.size(); k++) {
											Map<String, Object> areaMap = areaProList.get(k);
											String cityid = MapUtils.getString(areaMap, "area_code", "0");
											BigDecimal proportion = new BigDecimal((int) areaMap.get("proportion"));
											BigDecimal unit = new BigDecimal(100000);
											BigDecimal dayPlayNumAdd = new BigDecimal(Math.abs(diffnum) * 1000);
											if (diffnum < 0) {
												// 增终端
												double cityAddPlaytimes = dayPlayNumAdd.multiply(proportion)
														.divide(unit).doubleValue();
												String sqlAddMacAreaD = selectAddMacArea.replace("@daytime", daytime);
												String sqlAddMacAreaDT = sqlAddMacAreaD.replace("@tagid", tagid);
												String sqlAddMacAreaDTO = sqlAddMacAreaDT.replace("@oemid",String.valueOf(oemid));
												String sqlAddMacAreaDTOT = sqlAddMacAreaDTO.replace("@cityid", cityid);
												String sqlAddMacAreaDTOTP = sqlAddMacAreaDTOT.replace("@planid", String.valueOf(planid));
												String replace = refreshTable.replace("@table", "startpre_mac_dict");
												impalaJt.execute(replace);
												List<Map<String, Object>> addMacAreaList = impalaJt.queryForList(sqlAddMacAreaDTOTP);
												if (addMacAreaList != null && !addMacAreaList.isEmpty()) {
													double sum = 0.00;
													int count = 0;
													for (int m = 0; m < addMacAreaList.size(); m++) {
														Map<String, Object> addMacMap = addMacAreaList.get(m);
														double num = MapUtils.getDoubleValue(addMacMap, "num", 0.00);
														sum = sum + num;
														String mac = MapUtils.getString(addMacMap, "mac");
														rsmacList.add(mac);
														count++;
														if (sum >= cityAddPlaytimes) {
															break;
														}
														if (count == addMacAreaList.size()) {
															if (sum < cityAddPlaytimes) {
																logger.info("----------------------无区域限制情况下,渠道:" + oemid
																		+ "_城市:" + cityid + "需要增加的终端量不足,差"
																		+ (cityAddPlaytimes - sum)
																		+ "----------------------");
															}
														}
													}
													rsmacMap.put(String.valueOf(planid), rsmacList);
													storeMacToRredis.storeAddMacToFile(rsmacMap, daytime,
															String.valueOf(planid));
													storeMacToRredis.storeMacToRedis(daytime, String.valueOf(planid), "add");
												} else {
													logger.info("----------------------无区域限制下，无符合" + "/渠道：" + oemid
															+ "/城市：" + cityid + "增加的投放终端mac----------------------");
												}
											} else {
												// 删除终端
												double cityAddPlaytimes = dayPlayNumAdd.multiply(proportion)
														.divide(unit).doubleValue();
												String sqlAddMacAreaD = selectAddMacArea.replace("@daytime", daytime);
												String sqlAddMacAreaDT = sqlAddMacAreaD.replace("@tagid", tagid);
												String sqlAddMacAreaDTO = sqlAddMacAreaDT.replace("@oemid",
														String.valueOf(oemid));
												String sqlAddMacAreaDTOT = sqlAddMacAreaDTO.replace("@cityid", cityid);
												String sqlAddMacAreaDTOTP = sqlAddMacAreaDTOT.replace("@planid", String.valueOf(planid));
												String replace = refreshTable.replace("@table", "startpre_mac_dict");
												impalaJt.execute(replace);
												List<Map<String, Object>> addMacAreaList = impalaJt.queryForList(sqlAddMacAreaDTOTP);
												if (addMacAreaList != null && !addMacAreaList.isEmpty()) {
													double sum = 0.00;
													int count = 0;
													for (int m = 0; m < addMacAreaList.size(); m++) {
														Map<String, Object> addMacMap = addMacAreaList.get(m);
														double num = MapUtils.getDoubleValue(addMacMap, "num", 0.00);
														sum = sum + num;
														String mac = MapUtils.getString(addMacMap, "mac");
														rsmacList.add(mac);
														count++;
														if (sum >= cityAddPlaytimes) {
															break;
														}
														if (count == addMacAreaList.size()) {
															if (sum < cityAddPlaytimes) {
																logger.info("----------------------无区域限制情况下,渠道:" + oemid
																		+ "_城市:" + cityid + "需要增加的终端量不足,差"
																		+ (cityAddPlaytimes - sum)
																		+ "----------------------");
															}
														}
													}
													rsmacMap.put(String.valueOf(planid), rsmacList);
													storeMacToRredis.storeDelMacToFile(rsmacMap, daytime,
															String.valueOf(planid));
													storeMacToRredis.storeMacToRedis(daytime, String.valueOf(planid), "del");
												} else {
													logger.info("----------------------无区域限制下，无符合" + "/渠道：" + oemid
															+ "/城市：" + cityid + "删除的投放终端mac----------------------");
												}
											}
										}
									}
								}
							}
						} else {
							// 某创意，某区域报警情况
							int areaflag = MapUtils.getIntValue(prePlanMap, "areaflag");
							if (areaflag == 1) {
								// 区域限制
								String cityid = MapUtils.getString(prePlanMap, "cityid");
								List<Date> days = UtilityClass.findDates(new Date(), prePlanStoptime);
								for (int k = 0; k < days.size(); k++) {
									DateTime day = new DateTime(days.get(k));
									String tagid = String.valueOf(day.getDayOfWeek());
									String daytime = sdf1.format(day);
									// 该排期对应的投放区域的个数
									int areaCount = prePlanList.size();
									// 判断是新增还是删除终端操作
									if (diffnum < 0) {
										// 增终端----每个区域调整的量
										int addNum = (int) Math.rint(Math.abs(diffnum) * 1000 / areaCount);
										// 按媒体、oemid、市、tagid进行hive终端查询
										String sqlSelectMacD = selectAddMacArea.replace("@daytime", daytime);
										String sqlSelectMacDO = sqlSelectMacD.replace("@oemid", String.valueOf(oemid));
										String sqlSelectMacDOT = sqlSelectMacDO.replace("@tagid", tagid);
										String sqlSelectMacDOTC = sqlSelectMacDOT.replace("@cityid", cityid);
										String sqlSelectMacDOTCP = sqlSelectMacDOTC.replace("@planid", String.valueOf(planid));
										String replace = refreshTable.replace("@table", "startpre_mac_dict");
										impalaJt.execute(replace);
										List<Map<String, Object>> macDOTCList = impalaJt.queryForList(sqlSelectMacDOTCP);
										if (macDOTCList != null && !macDOTCList.isEmpty()) {
											double sum = 0.00;
											int count = 0;
											for (int l = 0; l < macDOTCList.size(); l++) {
												Map<String, Object> macMap = macDOTCList.get(l);
												double num = MapUtils.getDoubleValue(macMap, "num", 0.00);
												sum = sum + num;
												String mac = MapUtils.getString(macMap, "mac");
												rsmacList.add(mac);
												count++;
												if (sum >= addNum) {
													break;
												}
												if (count == macDOTCList.size()) {
													if (sum < addNum) {
														logger.info("----------------------区域限制情况下,渠道:" + oemid + "_城市:"
																+ cityid + "终端量不足,差" + (addNum - sum)
																+ "----------------------");
													}
												}
											}
											rsmacMap.put(String.valueOf(planid), rsmacList);
											// 存储到redis
											storeMacToRredis.storeAddMacToFile(rsmacMap, daytime, String.valueOf(planid));
											storeMacToRredis.storeMacToRedis(daytime, String.valueOf(planid), "add");
										} else {
											logger.info("------------------------区域限制情况下,渠道:" + oemid + "_城市:" + cityid
													+ "无符合要求的终端增加------------------------");
										}

									} else {
										// 删终端 ----每个区域调整的量
										int delNum = (int) Math.rint(diffnum * 1000 / areaCount);
										String sqlDMA = selectDelMacArea.replace("@daytime", daytime);
										String sqlDMAT = sqlDMA.replace("@tagid", tagid);
										String sqlDMATO = sqlDMAT.replace("@oemid", String.valueOf(oemid));
										String sqlDMATOC = sqlDMATO.replace("@cityid", cityid);
										String sqlDMATOCP = sqlDMATOC.replace("@planid", String.valueOf(planid));
										String replace = refreshTable.replace("@table", "startpre_mac_dict");
										impalaJt.execute(replace);
										List<Map<String, Object>> macDOTCList = impalaJt.queryForList(sqlDMATOCP);
										if (macDOTCList != null && !macDOTCList.isEmpty()) {
											double sum = 0.00;
											int count = 0;
											for (int l = 0; l < macDOTCList.size(); l++) {
												Map<String, Object> macMap = macDOTCList.get(l);
												double num = MapUtils.getDoubleValue(macMap, "num", 0.00);
												sum = sum + num;
												String mac = MapUtils.getString(macMap, "mac");
												rsmacList.add(mac);
												count++;
												if (sum >= delNum) {
													break;
												}
												if (count == macDOTCList.size()) {
													if (sum < delNum) {
														logger.info("----------------------区域限制情况下,渠道:" + oemid + "_城市:"
																+ cityid + "需要删除的终端量不足,差" + (delNum - sum)
																+ "----------------------");
													}
												}
											}
											rsmacMap.put(String.valueOf(planid), rsmacList);
											storeMacToRredis.storeDelMacToFile(rsmacMap, daytime, String.valueOf(planid));
											storeMacToRredis.storeMacToRedis(daytime, String.valueOf(planid), "del");
										} else {
											logger.info("------------------------区域限制情况下,渠道:" + oemid + "_城市:" + cityid
													+ "无符合要求的终端删除------------------------");
										}

									}
								}
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void start() {

	}

	@Override
	public void process(String time) {
		try {
			selectRealtimeMac();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {

	}

	public void setRefreshTable(String refreshTable) {
		this.refreshTable = refreshTable;
	}

	public void setSelectPrePlanInfo(String selectPrePlanInfo) {
		this.selectPrePlanInfo = selectPrePlanInfo;
	}

	public void setSelectAddMacArea(String selectAddMacArea) {
		this.selectAddMacArea = selectAddMacArea;
	}

	public void setSelectDelMacArea(String selectDelMacArea) {
		this.selectDelMacArea = selectDelMacArea;
	}

	public void setSelectAreaProp(String selectAreaProp) {
		this.selectAreaProp = selectAreaProp;
	}

	public void setRealtimeAlarm(RealtimeSelectAlarmInfo realtimeAlarm) {
		this.realtimeAlarm = realtimeAlarm;
	}

	public void setAdSupersspJt(JdbcTemplate adSupersspJt) {
		this.adSupersspJt = adSupersspJt;
	}

	public void setHiveJt(JdbcTemplate hiveJt) {
		this.hiveJt = hiveJt;
	}

	public void setImpalaJt(JdbcTemplate impalaJt) {
		this.impalaJt = impalaJt;
	}

	public void setStoreMacToRredis(StoreMacRredis storeMacToRredis) {
		this.storeMacToRredis = storeMacToRredis;
	}
	
}
