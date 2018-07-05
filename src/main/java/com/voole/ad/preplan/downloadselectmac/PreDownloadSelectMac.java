package com.voole.ad.preplan.downloadselectmac;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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

/**
 * 根据预下载mac汇报对已挑选mac的排期进行mac的增删调整，来满足正式商投 排期的曝光要求
 * 
 * @author liuyaxu
 * 
 *
 */
public class PreDownloadSelectMac implements IJobLife {

	Logger logger = Logger.getLogger(PreDownloadSelectMac.class);
	
	private String refreshTable;

	private String selectPlan;// 根据有效的预排期查询排期信息

	private String selectDetailPlan;// 查询有效预排期详细情况

	private String selectDownMacGroupCity;// 查询已下载终端对应的预排期的各区域曝光情况

	private String selectDownMacArea;// 查询未被占用的各区域终端信息
	
	private String selectAreaProp;

	private StoreMacRredis storeMacToRredis;
	@Autowired
	public JdbcTemplate adSupersspJt;

	@Autowired
	public JdbcTemplate hiveJt;
	
	@Autowired
	public JdbcTemplate impalaJt;
	/**
	 * 查询商投排期，在排期商投前一天进行排期的调整工作
	 */
	public void preDownload() {
		// 查询有效预排期对应的排期
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar calendarstart = Calendar.getInstance();
		calendarstart.setTime(new Date());
		calendarstart.set(Calendar.HOUR_OF_DAY, 0);
		calendarstart.set(Calendar.MINUTE, 0);
		calendarstart.set(Calendar.SECOND, 0);
		calendarstart.set(Calendar.MILLISECOND, 0);
		String time = sdf1.format(calendarstart.getTime());
		String selectPlanSql = selectPlan.replace("@plan_stoptime", time);
		List<Map<String, Object>> planList = adSupersspJt.queryForList(selectPlanSql);
		for (int i = 0; i < planList.size(); i++) {
			Map<String, Object> planMap = planList.get(i);
			String planDate = MapUtils.getString(planMap, "plan_starttime");
			Date today = new Date();
			Calendar calendar = new GregorianCalendar();
			try {
				calendar.setTime(sdf1.parse(planDate));
				calendar.add(calendar.DATE, -1);
				Date adjday = calendar.getTime();//商投排期前一天
				int n = UtilityClass.daysBetween(today, adjday);
				if (n == 1) {
					adjustMac();
				} else if (n < 0) {
					n = Math.abs(n);
					logger.info("------------------排期" + MapUtils.getString(planMap, "planid") + "已在前" + n + "天 进行调整-----------------");
				} else {
					logger.info("------------------排期" + MapUtils.getString(planMap, "planid") + "将在未来第" + n + "天 进行调整-----------------");
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	public void adjustMac() throws ParseException {
		/**
		 * 根据预下载终端汇报，调整对应的排期不满足终端需求的情况，另挑选终端，使CPM总量增幅在原曝光基础的5%左右
		 */
		List<Map<String, Object>> planDetailList = adSupersspJt.queryForList(selectDetailPlan);
		Map<String, List<String>> rsAddMacMap = new HashMap<String, List<String>>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date today = new Date();
		List<String> planidList = new ArrayList<String>();
		// 统计同一排期有区域限制，区域的个数，对于有区域限制的排期，暂且采用平均分配流量，挑选终端
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		if (planDetailList != null && !planDetailList.isEmpty()) {
			for (int i = 0; i < planDetailList.size() - 1; i++) {
				String planid = MapUtils.getString(planDetailList.get(i), "planid");
				planidList.add(planid);
			}
			for (String planid : planidList) {
				if (countMap.containsKey(planid)) {
					countMap.put(planid, countMap.get(planid).intValue() + 1);
				} else {
					countMap.put(planid, 1);
				}
			}
			// 根据排期日期，过滤已完成投放排期
			List<String> rsAddMacList = new ArrayList<String>();
			for (int i = 0; i < planDetailList.size(); i++) {
				Map<String, Object> planMap = planDetailList.get(i);
				String plan_stopTime = MapUtils.getString(planMap, "plan_stoptime");
				Date pStoptime = sdf.parse(plan_stopTime);
				int n = UtilityClass.daysBetween(pStoptime, today);
				String planid = MapUtils.getString(planMap, "planid");
				if (n < 0) {
					int areaCount = MapUtils.getIntValue(countMap, planid);
					int oemidflag = MapUtils.getIntValue(planMap, "oemidflag");
					// 判断排期是否有oemid限制
					if (oemidflag == 1) {
						int playtimesflag = MapUtils.getIntValue(planMap, "playtimesflag");
						String spStarttime = MapUtils.getString(planMap, "plan_starttime");
						String oemid = MapUtils.getString(planMap, "oemid");
						Date pStarttime = sdf.parse(spStarttime);
						List<Date> days = UtilityClass.findDates(pStarttime, pStoptime);
						int areaflag = MapUtils.getIntValue(planMap, "areaflag");
						for (int j = 0; j < days.size(); j++) {
							DateTime day = new DateTime(days.get(j).getTime());
							//排期的具体每日日期
							String daytime = sdf.format(days.get(j));
							String tagid = String.valueOf(day.getDayOfWeek());
							// 查询已下载终端对应的预排期的各区域曝光情况:planid---------cityid,playtimes
							String selectDMacGCP = selectDownMacGroupCity.replace("@planid", planid);
							String selectDMacGCPT = selectDMacGCP.replace("@tagid", tagid);
							//数据格式cityid , playtimes
							String replace = refreshTable.replace("@table", "startpre_mac_dict");
							impalaJt.execute(replace);
							List<Map<String, Object>> downMacList = impalaJt.queryForList(selectDMacGCPT);
							//计算 排期planid的预下载总曝光情况
							double playtimes = 0.00;
							for (Map<String, Object> downMacmap : downMacList) {
								for (String city : downMacmap.keySet()) {
									double playtime = MapUtils.getDoubleValue(downMacmap, city);
									playtimes = playtimes + playtime;
								}
							}
							// 按投放量限制，0：不限 1：按总量 2：按日 ----统计预下载上报排期，各区域上报mac情况，跟实际挑选的mac进行相关比较
							if (playtimesflag == 1) { 
								int timelength = UtilityClass.daysBetween(pStarttime, pStoptime);
								double dayplaytimes = MapUtils.getDoubleValue(planMap, "playtimesthreshold") / timelength;// 排期每日所有区域的曝光需求
								double adayplaytimes = dayplaytimes * (1.00+0.10);//下载终端为110%
								if (playtimes < adayplaytimes) {
									// 是否区域限制，0：不限 1：限制，地市
									if (areaflag == 1) {
										// 有区域限制，对于下载mac曝光 <= 每日所需的曝光
										double dayplaytime = adayplaytimes / areaCount; // 排期每日每个区域的曝光需求
										for (Map<String, Object> downMacmap : downMacList) {
											for (String city : downMacmap.keySet()) {
												double downplaytime = MapUtils.getDoubleValue(downMacmap, city);
												if (downplaytime < dayplaytime) {
													double addplaytime = dayplaytime - downplaytime;
													String cityid = city;
													String selectDMacAD = selectDownMacArea.replace("@daytime",daytime);
													String selectDMacADT = selectDMacAD.replace("@tagid", tagid);
													String selectDMacADTO = selectDMacADT.replace("@oemid", oemid);
													String selectDMacADTOC = selectDMacADTO.replace("@cityid",cityid);
													String selectDMacADTOCP = selectDMacADTOC.replace("@planid", planid);
													List<Map<String, Object>> addMacList = impalaJt.queryForList(selectDMacADTOCP);
													if (addMacList != null && !addMacList.isEmpty()) {
														double sum = 0.00;
														int count = 0;
														for (int k = 0; k < addMacList.size(); k++) {
															Map<String, Object> addMacMap = addMacList.get(i);
															double num = MapUtils.getDoubleValue(addMacMap, "num",0.00);
															String mac = MapUtils.getString(addMacMap, "mac");
															sum = sum + num;
															count++;
															rsAddMacList.add(mac);
															// ??终端量不足
															if (sum >= addplaytime) {
																break;
															}
															if (count == addMacList.size()) {
																if (sum < addplaytime) {
																	logger.warn("----------------------按总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + cityid + "的mac终端库存不足----------------------");
																}
															}
														}
														rsAddMacMap.put(planid, rsAddMacList);
														// 将新增的mac，记录到hive表preplan_mac_status并请求接口存储到redis
														storeMacToRredis.storeAddMacToFile(rsAddMacMap, daytime, planid);//需要将daytime新挑选的终端放到"daytime".txt,文件，及有效状态标记存储为daytime分区
														storeMacToRredis.storeMacToRedis(daytime, planid, "add");
														logger.info("----------------------按总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市" + city + "增加mac终端完成----------------------");
													} else {
														logger.warn("----------------------按总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + cityid + "无剩余mac终端库存----------------------");
													}
												} else {
													logger.info("----------------------按总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市" + city + "mac满足 "+ dayplaytime +"的mac需求----------------------");
												}
											}
										}
									} else {
										// 无区域限制,获取对应oemid下的各区域占比
										String selectDMAP= selectAreaProp.replace("@oemid", oemid);
										//数据格式：media_id,oemid,area_code,proportion
										List<Map<String, Object>> areaProList = adSupersspJt.queryForList(selectDMAP);
										for (int k = 0; k < areaProList.size(); k++) {
											Map<String, Object> areaProMap = areaProList.get(k);
											String cityid = MapUtils.getString(areaProMap, "area_code");
											BigDecimal proportion = new BigDecimal(MapUtils.getIntValue(areaProMap, "proportion"));
											BigDecimal allPlaytimes = new BigDecimal(dayplaytimes);
											BigDecimal unit = new BigDecimal(100000);
											//排期应选的对应到区域的曝光数
											double cityPlaytimes = allPlaytimes.multiply(proportion).divide(unit).doubleValue();
											for (int l = 0; l < downMacList.size(); l++) {
												Map<String, Object> downMacMap = downMacList.get(l);
												String downCity = MapUtils.getString(downMacMap, "cityid");
												if (cityid.equals(downCity)) {
													double downcityPlaytimes = MapUtils.getDoubleValue(downMacMap, "playtimesthreshold");
													//排期预下载汇报上来的对应到区域的曝光数据
													if (downcityPlaytimes < cityPlaytimes) {
														double addCityPlaytimes = cityPlaytimes - downcityPlaytimes;
														String selectDMacAD = selectDownMacArea.replace("@daytime",daytime);
														String selectDMacADT = selectDMacAD.replace("@tagid", tagid);
														String selectDMacADTO = selectDMacADT.replace("@oemid", oemid);
														String selectDMacADTOC = selectDMacADTO.replace("@cityid",cityid);
														String selectDMacADTOCP = selectDMacADTOC.replace("@planid", planid);
														List<Map<String, Object>> addMacList = impalaJt.queryForList(selectDMacADTOCP);
														if (addMacList != null && !addMacList.isEmpty()) {
															double sum = 0.00;
															int count = 0;
															for (int m = 0; m < addMacList.size(); k++) {
																Map<String, Object> addMacMap = addMacList.get(i);
																double num = MapUtils.getDoubleValue(addMacMap, "num",0.00);
																String mac = MapUtils.getString(addMacMap, "mac");
																sum = sum + num;
																rsAddMacList.add(mac);
																count++;
																// ??终端量不足
																if (sum >= addCityPlaytimes) {
																	break;
																}
																if (count == addMacList.size() ) {
																	if (sum < addCityPlaytimes) {
																		logger.warn("----------------------按总量投放，无区域限制排期" + planid + "/日期" +  daytime + "/城市：" + downCity + "的mac终端库存不足----------------------");
																	}
																}
																rsAddMacMap.put(planid, rsAddMacList);
																// 将新增的mac，记录到hive表preplan_mac_status并请求接口存储到redis
																storeMacToRredis.storeAddMacToFile(rsAddMacMap, daytime, planid);//需要将daytime新挑选的终端放到"daytime".txt,文件，及有效状态标记存储为daytime分区
																storeMacToRredis.storeMacToRedis(daytime, planid, "add");
																logger.info("----------------------按总量投放，无区域限制排期：" + planid + "/日期：" + daytime+ "/城市：" + downCity + "增加mac终端完成----------------------");
															}
														} else {
															logger.warn("----------------------按总量投放，无区域限制排期" + planid + "/日期" +  daytime + "/城市：" + downCity + "无剩余mac终端库存----------------------");
														}
													} else {
														logger.info("----------------------按总量投放，无区域限制排期" + planid + "/日期："+ daytime + "分配到" + downCity + "地区的mac，预下载上报的曝光满足要求----------------------");
													}
													
												}
											}
										}
									}
								} else {
									logger.info("----------------------按总量投放，排期：" + planid + "/日期：" + daytime + "/满足每日" + dayplaytimes + " 的曝光需求----------------------");
								}
							} else if (playtimesflag == 2) {
								// 按日总量 ----统计预下载上报排期，各区域上报mac情况，跟实际挑选的mac进行相关比较
								double dayplaytimes = MapUtils.getDoubleValue(planMap, "playtimesthreshold");
								double adayplaytime = dayplaytimes * (1.00+0.10);//下载终端为110%
								if (playtimes < adayplaytime) {
									// 是否区域限制，0：不限 1：限制，地市
									if (areaflag == 1) {
										// 有区域限制 对于下载mac曝光 <= 每日所需的曝光
										double dayplaytime = adayplaytime; // 排期每日每个区域的曝光需求
										for (Map<String, Object> downMacmap : downMacList) {
											for (String city : downMacmap.keySet()) {
												double playtime = MapUtils.getDoubleValue(downMacmap, city);
												if (playtime < dayplaytime) {
													double addplaytime = dayplaytime - playtime;
													String cityid = city;
													String selectDMacAD = selectDownMacArea.replace("@daytime",daytime);
													String selectDMacADT = selectDMacAD.replace("@tagid", tagid);
													String selectDMacADTO = selectDMacADT.replace("@oemid", oemid);
													String selectDMacADTOC = selectDMacADTO.replace("@cityid",cityid);
													String selectDMacADTOCP = selectDMacADTOC.replace("@planid", planid);
													List<Map<String, Object>> addMacList = impalaJt.queryForList(selectDMacADTOCP);
													if (addMacList != null && !addMacList.isEmpty()) {
														double sum = 0.00;
														int count = 0;
														for (int k = 0; k < addMacList.size(); k++) {
															Map<String, Object> addMacMap = addMacList.get(i);
															double num = MapUtils.getDoubleValue(addMacMap, "num",0.00);
															String mac = MapUtils.getString(addMacMap, "mac");
															sum = sum + num;
															rsAddMacList.add(mac);
															count++;
															// ??终端量不足
															if (sum >= addplaytime) {
																break;
															}
															if (count == addMacList.size()) {
																if (sum < addplaytime) {
																	logger.warn("----------------------按日总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + cityid + "的mac终端库存不足----------------------");
																}
															}
														}
														rsAddMacMap.put(planid, rsAddMacList);
														// 将新增的mac，记录到hive表preplan_mac_status并请求接口存储到redis
														storeMacToRredis.storeAddMacToFile(rsAddMacMap, daytime, planid);//需要将daytime新挑选的终端放到"daytime".txt,文件，及有效状态标记存储为daytime分区
														storeMacToRredis.storeMacToRedis(daytime, planid, "add");
														logger.info("----------------------按日总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市" + city + "增加mac终端完成----------------------");
													} else {
														logger.warn("----------------------按日总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + cityid + "无剩余mac终端库存----------------------");
													}
												} else {
													logger.info("----------------------按日总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + city + "mac满足 "+ dayplaytime +"的mac需求----------------------");
												}
											}
										}
									} else {
										// 无区域限制,获取对应oemid下的各区域占比
										String selectDMAP= selectAreaProp.replace("@oemid", oemid);
										List<Map<String, Object>> areaProList = adSupersspJt.queryForList(selectDMAP);
										for (int k = 0; k < areaProList.size(); k++) {
											Map<String, Object> areaProMap = areaProList.get(k);
											String cityid = MapUtils.getString(areaProMap, "area_code");
											BigDecimal proportion = new BigDecimal(MapUtils.getIntValue(areaProMap, "proportion"));
											BigDecimal allPlaytimes = new BigDecimal(dayplaytimes);
											BigDecimal unit = new BigDecimal(100000);
											//排期对应到区域的曝光数
											double cityPlaytimes = allPlaytimes.multiply(proportion).divide(unit).doubleValue();
											for (int l = 0; l < downMacList.size(); l++) {
												Map<String, Object> downMacMap = downMacList.get(l);
												String downCity = MapUtils.getString(downMacMap, "cityid");
												if (cityid.equals(downCity)) {
													double downcityPlaytimes = MapUtils.getDoubleValue(downMacMap, "playtimesthreshold");
													//排期预下载汇报上来的对应到区域的曝光数据
													if (downcityPlaytimes < cityPlaytimes) {
														double addCityPlaytimes = cityPlaytimes - downcityPlaytimes;
														String selectDMacAD = selectDownMacArea.replace("@daytime",daytime);
														String selectDMacADT = selectDMacAD.replace("@tagid", tagid);
														String selectDMacADTO = selectDMacADT.replace("@oemid", oemid);
														String selectDMacADTOC = selectDMacADTO.replace("@cityid",cityid);
														String selectDMacADTOCP = selectDMacADTOC.replace("@planid", planid);
														List<Map<String, Object>> addMacList = impalaJt.queryForList(selectDMacADTOCP);
														if (addMacList != null && !addMacList.isEmpty()) {
															double sum = 0.00;
															int count = 0;
															for (int m = 0; m < addMacList.size(); k++) {
																Map<String, Object> addMacMap = addMacList.get(i);
																double num = MapUtils.getDoubleValue(addMacMap, "num",0.00);
																String mac = MapUtils.getString(addMacMap, "mac");
																sum = sum + num;
																rsAddMacList.add(mac);
																count++;
																// ??终端量不足
																if (sum >= addCityPlaytimes) {
																	break;
																}
																if (count == addMacList.size()) {
																	if (sum < addCityPlaytimes) {
																		logger.warn("----------------------按日总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + downCity + "的mac终端库存不足----------------------");
																	}
																}
																rsAddMacMap.put(planid, rsAddMacList);
																// 将新增的mac，记录到hive表preplan_mac_status并请求接口存储到redis
																storeMacToRredis.storeAddMacToFile(rsAddMacMap, daytime, planid);//需要将daytime新挑选的终端放到"daytime".txt,文件，及有效状态标记存储为daytime分区
																storeMacToRredis.storeMacToRedis(daytime, planid, "add");
																logger.info("----------------------按日总量投放，无区域要求的排期：" + planid + "/日期：" + daytime + "/城市" + downCity + "增加mac终端完成----------------------");
															}
														} else {
															logger.warn("----------------------按日总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + downCity + "无剩余mac终端库存----------------------");
														}
													} else {
														logger.info("----------------------按日总量投放，有区域要求的排期：" + planid + "/日期：" + daytime + "/城市：" + downCity + "mac满足 "+ cityPlaytimes +"的mac需求----------------------");
													}
												}
											}
										}
									}
								} else {
									logger.info("----------------------按日总量投放，排期：" + planid + "/日期：" + daytime + "/满足每日" + dayplaytimes + " 的曝光需求----------------------");
								}
							} else {
								logger.warn("----------------------排期" + planid + "无投放量限制，请检查排期需求(不允许无投放量限制)----------------------");
							}
						}
					} else {
						logger.warn("----------------------排期" + planid + "无oemid限制，请检查排期拆分后要求(不允许无oemid限制)----------------------");
					}
				} else if (n == 0) {
					logger.info("----------------------排期" + planid + "今天:"+sdf.format(today)+"结束----------------------");
				} else {
					logger.warn("----------------------排期" + planid + "已结束投放，请检查生成预排期的代码功能----------------------");
				}
			}
		} else {
			logger.info("----------------------无广告排期----------------------");
		}
	}

	@Override
	public void start() {

	}

	@Override
	public void process(String time) {
		preDownload();
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

	public void setSelectDetailPlan(String selectDetailPlan) {
		this.selectDetailPlan = selectDetailPlan;
	}

	public void setSelectDownMacGroupCity(String selectDownMacGroupCity) {
		this.selectDownMacGroupCity = selectDownMacGroupCity;
	}

	public void setSelectDownMacArea(String selectDownMacArea) {
		this.selectDownMacArea = selectDownMacArea;
	}

	public void setSelectAreaProp(String selectAreaProp) {
		this.selectAreaProp = selectAreaProp;
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
