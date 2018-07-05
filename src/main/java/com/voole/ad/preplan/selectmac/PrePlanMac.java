package com.voole.ad.preplan.selectmac;
/**
 * 根据排期挑选广告预下载mac
 * 将挑选的mac存储到redis
 * 在表preplan_mac_status记录有排期的mac终端
 * @author liuyaxu
 */

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

public class PrePlanMac implements IJobLife {

	Logger logger = Logger.getLogger(PrePlanMac.class);
	private String refreshTable;
	
	private String selectPlan;// 根据有效的预排期查询排期信息
	
	private String selectStorePlan;

	private String selectMacO;

	private String selectAreaProp;

	private String updateAdplan;
	
	private StoreMacRredis storeMacToRredis;
	
	@Autowired
	public JdbcTemplate adSupersspJt;

	@Autowired
	public JdbcTemplate hiveJt;
	
	@Autowired
	public JdbcTemplate impalaJt;
	//查询有效排期，选择相应的mac存储到文件中
	public void selectPlanList() throws ParseException {
		logger.info("-------------------------查询有效预排期对应的排期要求-------------------------");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		String time = sdf.format(calendar.getTime());
		String selectPlanSql = selectPlan.replace("@plan_stoptime", time);
		List<Map<String, Object>> planList = adSupersspJt.queryForList(selectPlanSql);// 查询有效预排期对应的排期要求
		Map<String, List<String>> rsmacMap = new HashMap<String, List<String>>();
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		List<String> planidList = new ArrayList<String>();
		Map<String, Integer> countMap = new HashMap<String,Integer>();
		if (planList != null && !planList.isEmpty()) {
			for (int i = 0; i < planList.size(); i++) {
				String planid = MapUtils.getString(planList.get(i), "planid");
				planidList.add(planid);
			}
			for (String planid : planidList) {
				if (countMap.containsKey(planid)) {
					countMap.put(planid, countMap.get(planid).intValue() + 1);
				} else {
					countMap.put(planid, 1);
				}
			}
			for (int i = 0; i < planList.size(); i++) {
				List<String> rsmacList = new ArrayList<String>();
				Map<String, Object> map = planList.get(i);
				String planid = MapUtils.getString(map, "planid");
				int areaCount = MapUtils.getIntValue(countMap, planid);//将总量平均分给区域的个数
				// 按投放量限制，0：不限 1：按总量 2：按日
				boolean flag = false;
				int playtimesflag = MapUtils.getIntValue(map, "playtimesflag");
				if (playtimesflag == 1) {
					String pStarttime = MapUtils.getString(map, "plan_starttime");
					String pStoptime = MapUtils.getString(map, "plan_stoptime");
					Date p_Starttime = sdf.parse(pStarttime);
					Date p_Stoptime = sdf.parse(pStoptime);
					int timelength = UtilityClass.daysBetween(p_Starttime, p_Stoptime);
					double dayplaytimes = (double) Math.rint(MapUtils.getInteger(map, "playtimesthreshold") / timelength / areaCount);// 每日曝光需求
					List<Date> days = UtilityClass.findDates(p_Starttime, p_Stoptime);
					for (int j = 0; j < days.size(); j++) {
						DateTime day = new DateTime(days.get(j).getTime());
						String daytime = sdf1.format(days.get(j));
						String tagid = String.valueOf(day.getDayOfWeek());
						int oemidflag = MapUtils.getIntValue(map, "oemidflag");
						int areaflag = MapUtils.getIntValue(map, "areaflag");
						// 判断投放日期为星期几,限制oemid渠道
						if (oemidflag == 1) {
							String oemid = MapUtils.getString(map, "oemid");
							// 是否区域限制，0：不限 1：限制，地市
							String selectMacOD = selectMacO.replaceAll("@daytime", daytime);
							String selectMacODT = selectMacOD.replaceAll("@tagid", tagid);
							String selectMacODTMO = selectMacODT.replaceAll("@oemid", oemid);
							if (areaflag == 1) {
								String provinceid = MapUtils.getString(map, "provinceid", "0");
								String selectMacODTMOP = selectMacODTMO.replaceAll("@provinceid", provinceid);
								String cityid = MapUtils.getString(map, "cityid", "0");
								String selectMacODTMOPC = selectMacODTMOP.replaceAll("@cityid", cityid);
								// 按媒体、oemid、省、市、周末版进行hive终端查询
								String replace = refreshTable.replace("@table", "startpre_mac_dict");
								impalaJt.execute(replace);
								List<Map<String, Object>> macODTMOPCList = impalaJt.queryForList(selectMacODTMOPC);
								if (macODTMOPCList != null && !macODTMOPCList.isEmpty()) {
									double sum = 0.00;
									int count = 0;
									for (int k = 0; k < macODTMOPCList.size(); k++) {
										Map<String, Object> mapODTMOPC = macODTMOPCList.get(k);
										double num = MapUtils.getDoubleValue(mapODTMOPC, "num", 0.00);
										sum = sum + num;
										String mac = MapUtils.getString(mapODTMOPC, "mac");
										rsmacList.add(mac);
										count++;
										// ????mac足够的情况
										if (sum >= dayplaytimes) {
											break;
										}
										if (count == macODTMOPCList.size()) {
											if (sum < dayplaytimes) {
												logger.info("----------------------按总投放量,有区域限制情况下，" + "/渠道：" + oemid + "/城市：" + cityid + "mac库存量不足----------------------");
											}
										}
									}
									rsmacMap.put(planid, rsmacList);
									storeMacToRredis.storeAddMacToFile(rsmacMap,daytime,planid);
								} else {
									logger.info("----------------------按总投放量区域限制下，无符合" + "/渠道：" + oemid + "/城市："  + cityid+ "要求的投放mac----------------------");
								}
							} else {
								// 不限制区域
								String selectAreaProDO = selectAreaProp.replaceAll("@oemid", oemid);
								// 查询媒体、oemid的区域占比
								List<Map<String, Object>> areaProList = adSupersspJt.queryForList(selectAreaProDO);
								BigDecimal dayAllPlayNum = new BigDecimal(dayplaytimes);
								String selectMacODTMOP = selectMacODTMO.replace("and a.provinceid = @provinceid", " ");
								if (areaProList != null && !areaProList.isEmpty()) {
									for (int k = 0; k < areaProList.size(); k++) {
										Map<String, Object> areaMap = areaProList.get(k);
										String cityid = MapUtils.getString(areaMap, "area_code", "0");
										BigDecimal proportion = new BigDecimal((int) areaMap.get("proportion"));
										BigDecimal unit = new BigDecimal(100000);
										double cityPlaytimes = dayAllPlayNum.multiply(proportion).divide(unit).doubleValue();
										String selectMacAreaC = selectMacODTMOP.replaceAll("@cityid", cityid);
										String replace = refreshTable.replace("@table", "startpre_mac_dict");
										impalaJt.execute(replace);
										List<Map<String, Object>> macAreaCList = impalaJt.queryForList(selectMacAreaC);
										if (macAreaCList != null && !macAreaCList.isEmpty()) {
											double sum = 0.00;
											String planids = new String();
											int count = 0;
											for (int l = 0; l < macAreaCList.size(); l++) {
												Map<String, Object> mapAreaC = macAreaCList.get(l);
												double num = MapUtils.getDoubleValue(mapAreaC, "num", 0.00);
												sum = sum + num;
												String mac = (String) mapAreaC.get("mac");
												rsmacList.add(mac);
												count++;
												// ????mac足够的情况
												if (sum >= cityPlaytimes) {
													break;
												}
												if (count == macAreaCList.size()) {
													if (sum < cityPlaytimes) {
														logger.info("----------------------按日投放量,有区域限制情况下，" + "/渠道：" + oemid + "/城市：" + cityid + " mac库存量不足----------------------");
													}
												} 
											}
											planids = MapUtils.getString(map, "planid");
											rsmacMap.put(planids, rsmacList);
											storeMacToRredis.storeAddMacToFile(rsmacMap,daytime,planid);
										} else {
											logger.info("----------------------按总投放量无区域限制情况下，无符合" + "/渠道：" + oemid + "/城市：" + cityid + " 要求的投放mac----------------------");
										}
									}
								} else {
									logger.info("----------------------按总投放量无区域限制情况下，无" + "/渠道：" + oemid + "/" + "mac的区域占比情况----------------------");
								}
							}
						} else {
							logger.warn("----------------------排期" + map.get("planid") + "无oemid限制----------------------");
						}
						if (j == (days.size() - 1)) {
							flag = true;
						}
					}
				} else if (playtimesflag == 2) {// 计算按日需求
					String pStarttime = MapUtils.getString(map, "plan_starttime");
					String pStoptime = MapUtils.getString(map, "plan_stoptime");
					Date p_Starttime = sdf.parse(pStarttime);
					Date p_Stoptime = sdf.parse(pStoptime);
					double dayplaytimes = MapUtils.getDoubleValue(map, "playtimesthreshold", 0.00) / areaCount; // 每日曝光需求
					List<Date> days = UtilityClass.findDates(p_Starttime, p_Stoptime);
					for (int j = 0; j < days.size(); j++) {
						DateTime day = new DateTime(days.get(j).getTime());
						String tagid = String.valueOf(day.getDayOfWeek());
						String daytime = sdf1.format(days.get(j));
						int oemidflag = MapUtils.getIntValue(map, "oemidflag");
						int areaflag = MapUtils.getIntValue(map, "areaflag");
						// 判断投放日期为星期几,限制oemid渠道
						if (oemidflag == 1) {
							String oemid = MapUtils.getString(map, "oemid");
							String selectMacOD = selectMacO.replaceAll("@daytime", daytime);
							String selectMacODT = selectMacOD.replaceAll("@tagid", tagid);
							String selectMacODTO = selectMacODT.replaceAll("@oemid", oemid);
							// 是否区域限制，0：不限 1：限制，地市
							if (areaflag == 1) {
								String provinceid = MapUtils.getString(map, "provinceid", "0");
								String selectMacODTOP = selectMacODTO.replaceAll("@provinceid", provinceid);
								String cityid = MapUtils.getString(map, "cityid", "0");
								String selectMacODTOPC = selectMacODTOP.replaceAll("@cityid", cityid);
								// 按媒体、oemid、省、市、星期几进行hive终端查询
								String replace = refreshTable.replace("@table", "startpre_mac_dict");
								impalaJt.execute(replace);
								List<Map<String, Object>> macODTOPCList = impalaJt.queryForList(selectMacODTOPC);
								if (macODTOPCList != null && !macODTOPCList.isEmpty()) {
									double sum = 0.00;
									String planids = new String();
									int count = 0;
									for (int k = 0; k < macODTOPCList.size(); k++) {
										Map<String, Object> mapODTOPC = macODTOPCList.get(k);
										double num = MapUtils.getDoubleValue(mapODTOPC, "num", 0.0);
										sum = sum + num;
										String mac = MapUtils.getString(mapODTOPC, "mac");
										// 将mac和排期id存储到map
										rsmacList.add(mac);
										// ????mac足够的情况
										count++;
										if (sum >= dayplaytimes) {
											break;
										}
										if (count == macODTOPCList.size()) {
											if (sum < dayplaytimes) {
												logger.info("----------------------按日投放量,有区域限制情况下，" + "/渠道：" + oemid + "/城市：" + cityid + " mac库存量不足----------------------");
											}
										} 
									}
									planids = MapUtils.getString(map, "planid");
									rsmacMap.put(planids, rsmacList);
									storeMacToRredis.storeAddMacToFile(rsmacMap,daytime,planid);
									if (j == days.size()) {
										flag = true;
									}
								} else {
									logger.info("----------------------按日投放量,有区域限制下，无符合" + "/渠道：" + oemid + "/省：" +  "/城市：" + cityid + " 要求的投放mac----------------------");
								}
							} else {
								// 不限制区域
								String selectAreaPropD = selectAreaProp.replaceAll("@daytime", daytime);
								String selectAreaPropDO = selectAreaPropD.replaceAll("@oemid", oemid);
								// 查询媒体、oemid的区域占比
								String selectMacODTOC = selectMacODTO.replaceAll("and a.provinceid = @provinceid", "");
								List<Map<String, Object>> areaProList = adSupersspJt.queryForList(selectAreaPropDO);
								BigDecimal dayAllPlayNum = new BigDecimal(dayplaytimes);
								if (areaProList != null && !areaProList.isEmpty()) {
									for (int k = 0; k < areaProList.size(); k++) {
										Map<String, Object> areaMap = areaProList.get(k);
										String cityid = MapUtils.getString(areaMap, "area_code", "0");
										BigDecimal proportion = new BigDecimal((int) areaMap.get("proportion"));
										BigDecimal unit = new BigDecimal(100000);
										double cityPlaytimes = dayAllPlayNum.multiply(proportion).divide(unit).doubleValue();
										String selectMacAreaC = selectMacODTOC.replaceAll("@cityid", cityid);
										String replace = refreshTable.replace("@table", "startpre_mac_dict");
										impalaJt.execute(replace);
										List<Map<String, Object>> macAreaCList = impalaJt.queryForList(selectMacAreaC);
										if (macAreaCList != null && !macAreaCList.isEmpty()) {
											double sum = 0.00;
											String planids = new String();
											int count = 0;
											for (int l = 0; l < macAreaCList.size(); l++) {
												Map<String, Object> mapAreaC = macAreaCList.get(l);
												double num = MapUtils.getDouble(mapAreaC, "num", 0.00);
												sum = sum + num;
												String mac = MapUtils.getString(mapAreaC, "mac");
												rsmacList.add(mac);
												count++;
												// ????mac足够的情况
												if (sum >= cityPlaytimes) {
													break;
												}
												if (count == macAreaCList.size()) {
													if (sum < cityPlaytimes) {
														logger.info("----------------------按日投放量,无区域限制情况下，" + "/渠道：" + oemid + "/城市：" + cityid + " mac库存量不足----------------------");
													}
												} 
											}
											planids = MapUtils.getString(map, "planid");
											rsmacMap.put(planids, rsmacList);
											storeMacToRredis.storeAddMacToFile(rsmacMap,daytime,planid);
										}
									}
								} else {
									logger.info("----------------------按日总量，无区域限制情况下，无" + "/渠道：" + oemid + "/" + "mac的区域占比情况----------------------");
								}
							}
						} else {
							logger.warn("----------------------排期" +  planid  + "无oemid限制----------------------");
						}
						if (j == (days.size() - 1)) {
							flag = true;
						}
					}
				} else {
					logger.warn("----------------------排期" + planid + "无投放量限制，请检查排期----------------------");
				}
				if (flag == true) {
					String updateAdjust = updateAdplan.replaceAll("@planid", planid);
					int success = adSupersspJt.update(updateAdjust);
					if (success > 0) {
						logger.info("---------------------更改排期" + planid + "adjust状态为1成功---------------------");
					} else {
						logger.info("---------------------更改排期" + planid + "adjust状态为1失败---------------------");
					}
				} else {
					logger.info("存储失败");
				}
			}
		} else {
			logger.info("----------------------无需要选择mac的广告排期----------------------");
		}
	}
	
	public void storeMacToRedis() throws ParseException {
		logger.info("--------------------------将挑选完的终端存入redis和hdfs--------------------------");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		String time = sdf.format(calendar.getTime());
		String selectPlanSql = selectStorePlan.replace("@plan_stoptime", time);
		List<Map<String, Object>> planList = adSupersspJt.queryForList(selectPlanSql);
		if (planList != null && !planList.isEmpty()) {
			for (int i = 0; i < planList.size(); i++) {
				Map<String, Object> planMap = planList.get(i);
				String planid = MapUtils.getString(planMap, "planid");
				String planStarttime = MapUtils.getString(planMap, "plan_starttime");
				String planStoptime = MapUtils.getString(planMap, "plan_stoptime");
				Date startTime = sdf.parse(planStarttime);
				Date stopTime = sdf.parse(planStoptime);
				List<Date> planDateList = UtilityClass.findDates(startTime, stopTime);
				if (planDateList != null && !planDateList.isEmpty()) {
					for (int j = 0; j < planDateList.size(); j++) {
						String daytime = sdf1.format(planDateList.get(j));
						storeMacToRredis.storeMacToRedis(daytime, planid, "store");
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
			selectPlanList();
			storeMacToRedis();
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

	public void setSelectPlan(String selectPlan) {
		this.selectPlan = selectPlan;
	}

	public void setSelectStorePlan(String selectStorePlan) {
		this.selectStorePlan = selectStorePlan;
	}

	public void setSelectMacO(String selectMacO) {
		this.selectMacO = selectMacO;
	}

	public void setSelectAreaProp(String selectAreaProp) {
		this.selectAreaProp = selectAreaProp;
	}

	public void setUpdateAdplan(String updateAdplan) {
		this.updateAdplan = updateAdplan;
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
