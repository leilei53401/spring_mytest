package com.voole.ad.warning;

import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import com.voole.ad.main.IJobLife;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shaoyl on 2017-9-28.
 * 预估占比计算。
 * 根据权重和实际占比计算预估占比值
 * 同时计算预估值
 */


public class WarnCalRateJob implements IJobLife {
    private static Logger logger = Logger.getLogger(WarnCalRateJob.class);

    @Autowired
    public JdbcTemplate adSupersspJt;

    float historyWeight = 0.2f;
    float sameTermWeight = 0.3f;
    float yestodayWeight = 0.5f;

    String getProjectInfoSql; //获取预警预排期相关信息

    String getRealProportionSql;//获取实际占比数据sql

    String getEstimateProportionSql;//获取预估占比数据

    private String insertEstimateProportionSql;//记录预警占预估比值

    private String insertEstimateValueSql;//记录预警具体预估值


    @Override
    public void start() {

    }

    @Override
    public void process(String time) {

        DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime processDateTime = DateTime.parse(time, fromat);

        DateTime currDateStartTime = processDateTime.plusDays(1);

        //获取这一天开始时间
//        DateTime currDateStartTime = currDateTime.withDate(currDateTime.getYear(), currDateTime.getMonthOfYear(), currDateTime.getDayOfMonth());
        String todayStr = currDateStartTime.toString("yyyy-MM-dd");
        String todayTime = currDateStartTime.toString("yyyyMMdd");

        DateTime endDateTime = currDateStartTime.plusDays(1);

        //########## 处理时间 #########
        //历史时期
        DateTime hisStartTime = currDateStartTime.plusDays(-37);
        String histStartDayStr = hisStartTime.toString("yyyMMdd");
        DateTime hisEndTime = currDateStartTime.plusDays(-7);
        String histEndDayStr = hisEndTime.toString("yyyMMdd");
        //上周同期
        DateTime lastWeekTime = hisEndTime;
        String lastWeekDayStr = histEndDayStr;

        //昨天
        DateTime yestodayTime = currDateStartTime.plusDays(-1);
        String yestodayStr = yestodayTime.toString("yyyMMdd");


        //######################################################
        //获取本次投放的项目计划信息,计算本期要计算的预估占比。

        String getProjectInfoSqlNew = getProjectInfoSql.replaceAll("@today", todayStr);


        List<Map<String, Object>> projectPlanList = adSupersspJt.queryForList(getProjectInfoSqlNew);

        if (null != projectPlanList && projectPlanList.size() > 0) {

            logger.warn("获取到时间为[" + todayStr + "]的有效项目计划数据【" + projectPlanList.size() + "】条!");

            List<Map<String, String>> estimateProportionList = new ArrayList<Map<String, String>>();
            List<Map<String, String>> estimateValueList = new ArrayList<Map<String, String>>();

            for (Map<String, Object> projectPlan : projectPlanList) {

                String projectid = "";
                String oemid = "";
                String creativeid = "";
                String adptype = "";
                String amount = "";//计划曝光量

                try {
                    projectid = projectPlan.get("projectid").toString();
                    oemid = projectPlan.get("oemid").toString();
                    creativeid = projectPlan.get("creativeid").toString();
                    adptype = projectPlan.get("adptype").toString();
                    amount = projectPlan.get("amount").toString();
                } catch (Exception e) {
                    logger.warn("解析项目信息出错:",e);
                    logger.info("出错项目信息为："+projectPlan.toString());
                    continue;
                }

                //################# 循环计算每十分钟的预估占比数和具体阈值 ##########

                DateTime startTenTime = currDateStartTime;
                while (startTenTime.isBefore(endDateTime.getMillis())) {

                    DateTime endTentime = startTenTime.plusMinutes(10);
                    String endTenStr = endTentime.toString("yyyyMMddHHmmss");
                    String endTenPartStr = endTentime.toString("HHmmss");

                    /*
                    # 1、 先获取当前渠道和广告位当前时刻是否已计算预估占比数据
                    # 2、 如果已计算过，则直接使用占比数据计算阈值
                    # 3、 如果没有计算过，则开始计算并记录占比数据
                    */
                    float estimateProportion = 0.0f;

                    float estimateProportionDB = getEstimateProportionFromDB(todayTime, oemid, adptype, endTenStr);

                    if (estimateProportionDB > 0.0f) {
                        //已计算预估占比，直接使用
                        estimateProportion = estimateProportionDB;
                    } else {
                        //该时刻还未计算预估占比
                        float hisProportion = getRealProportionValue(histStartDayStr, histEndDayStr, oemid, creativeid, adptype, endTenPartStr);

                        float lastWeekProportion = getRealProportionValue(lastWeekDayStr, lastWeekDayStr, oemid, creativeid, adptype, endTenPartStr);

                        float yestodayProportion = getRealProportionValue(yestodayStr, yestodayStr, oemid, creativeid, adptype, endTenPartStr);


                        if (hisProportion > 0.0f || lastWeekProportion > 0.0f || yestodayProportion > 0.0f) {
                            //有不为0的去计算
                            estimateProportion = getEstimateProportion(hisProportion, lastWeekProportion, yestodayProportion);
                        } else {
                            //STODO: 全为0，没有历史记录占比的时候如何计算？(初始化默认占比数据，可提前计算几天的实际占比数据)
                            //无历史占比数据，暂时不预警
                            logger.warn("oemid=[" + oemid + "],adposid=[" + adptype + "] 暂无历史占比数据!");
                        }


                        //记录该时刻预估占比数据
                        if(estimateProportion>0.0) {
                            Map<String, String> estimateProportionMap = new HashMap<String, String>();
                            estimateProportionMap.put("oemid", oemid);
                            estimateProportionMap.put("mediaid", StringUtils.substring(oemid, 0, 6));
                            estimateProportionMap.put("adptype", adptype);
                            estimateProportionMap.put("proportion", estimateProportion + "");
                            estimateProportionMap.put("daytime", todayTime);
                            estimateProportionMap.put("moment", endTenStr);
                            estimateProportionList.add(estimateProportionMap);
                        }

                    }


                    //计算预估阈值
                    if(estimateProportion>0.0) {

                        long lAmount = Long.valueOf(amount);

                        long estimateValue = (long) (lAmount * estimateProportion);

                        //记录该时刻占比阈值
                        Map<String, String> estimateValueMap = new HashMap<String, String>();
                        estimateValueMap.put("projectid", projectid);
                        estimateValueMap.put("oemid", oemid);
                        estimateValueMap.put("mediaid", StringUtils.substring(oemid, 0, 6));
                        estimateValueMap.put("creativeid", creativeid);
                        estimateValueMap.put("adptype", adptype);
                        estimateValueMap.put("daytime", todayTime);
                        estimateValueMap.put("moment", endTenStr);
                        estimateValueMap.put("esvalue", estimateValue + "");
                        estimateValueList.add(estimateValueMap);

                    }

                    //更新时间
                    startTenTime = endTentime;
                }
            }

            //入库占比数据
            insertEstimateProportionData(estimateProportionList);

            //入库阈值数据
            insertEstimateValueData(estimateValueList);

        } else {
            logger.warn("未获取到日期为[" + todayStr + "]的项目排期计划信息。");
        }


    }

    @Override
    public void stop() {

    }

    /**
     * 获取实际占比数据
     *
     * @param startTime
     * @param endTime
     * @param oemid
     * @param creativeid
     * @param adptype
     * @param partMoment
     * @return
     */
    private float getRealProportionValue(String startTime, String endTime, String oemid, String creativeid, String adptype, String partMoment) {
        float proportionValue = 0.0f;
        String newSql = getRealProportionSql;
        newSql = newSql.replaceAll("@starttime", startTime).replaceAll("@endtime", endTime).replaceAll("@oemid", oemid)
                .replaceAll("@creativeid", creativeid).replaceAll("@adptype", adptype).replaceAll("@parttime", partMoment);


        List<Map<String, Object>> proportionValueList = adSupersspJt.queryForList(newSql);

        if (null != proportionValueList && proportionValueList.size() > 0) {

            for (Map<String, Object> proportionValueObj : proportionValueList) {
                String proportion = proportionValueObj.get("proportion").toString();
                if (StringUtils.isNotBlank(proportion)) {
                    proportionValue = Float.valueOf(proportion);
                }
            }

            logger.info("获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "], oemid=[" + oemid + "], creativeid=[" + creativeid + "], adposid=[" + adptype + "], partMoment=[" + partMoment + "],的占比数据为【" + proportionValue + "】");

        } else {
            logger.warn("未获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "], oemid=[" + oemid + "], creativeid=[" + creativeid + "], adposid=[" + adptype + "], partMoment=[" + partMoment + "],的数据");
        }

        return proportionValue;
    }


    /**
     * 获取当前时刻预估占比
     *
     * @param oemid
     * @param adptype
     * @param moment
     * @return
     */
    private float getEstimateProportionFromDB(String todayTime, String oemid, String adptype, String moment) {
        float proportionValue = 0.0f;
        String newSql = getEstimateProportionSql;
        newSql = newSql.replaceAll("@starttime", todayTime).replaceAll("@endtime", todayTime)
                .replaceAll("@oemid", oemid).replaceAll("@adptype", adptype).replaceAll("@moment", moment);

        List<Map<String, Object>> proportionValueList = adSupersspJt.queryForList(newSql);

        if (null != proportionValueList && proportionValueList.size() > 0) {

            for (Map<String, Object> proportionValueObj : proportionValueList) {
                String proportion = proportionValueObj.get("proportion").toString();
                if (StringUtils.isNotBlank(proportion)) {
                    proportionValue = Float.valueOf(proportion);
                }
            }

            logger.info("获取到 oemid=[" + oemid + "], adptype=[" + adptype + "], moment=[" + moment + "],的预估占比数据为【" + proportionValue + "】");

        } else {
            logger.warn("未获取到 oemid=[" + oemid + "], adposid=[" + adptype + "], moment=[" + moment + "]的预估占比数据");
        }

        return proportionValue;
    }


    /**
     * 根据权重计算预估占比值
     * 注：考虑数据不存在的情况下如何计算
     *
     * @return
     */
    private float getEstimateProportion(float hisProportion, float lastWeekProportion, float yestodayProportion) {
        float newHisWeight;
        if (0.0f == hisProportion) {
            newHisWeight = 0f;
        } else {
            newHisWeight = historyWeight;
        }

        float newSameTermWeight;
        if (0.0f == lastWeekProportion) {
            newSameTermWeight = 0f;
        } else {
            newSameTermWeight = sameTermWeight;
        }

        float newYestodayWeight;
        if (0.0f == yestodayProportion) {
            newYestodayWeight = 0f;
        } else {
            newYestodayWeight = yestodayWeight;
        }

        //总权重
        float newWeightAll = newHisWeight + newSameTermWeight + newYestodayWeight;

        float tmpHisProportion = 0.0f;
        float tmpLastWeekProportion = 0.0f;
        float tmpYestodayProportion = 0.0f;

        if (newWeightAll > 0.0f) {
            tmpHisProportion = hisProportion * newHisWeight / newWeightAll;
            tmpLastWeekProportion = lastWeekProportion * newSameTermWeight / newWeightAll;
            tmpYestodayProportion = yestodayProportion * newYestodayWeight / newWeightAll;
        }

        return tmpHisProportion + tmpLastWeekProportion + tmpYestodayProportion;
    }


    /**
     * 记录预估占比数据
     *
     * @param estimateProportionDataList
     */
    public void insertEstimateProportionData(final List<Map<String, String>> estimateProportionDataList) {

        int[] result = adSupersspJt.batchUpdate(insertEstimateProportionSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, String> estimateProportionMap = estimateProportionDataList.get(i);
                ps.setString(1, estimateProportionMap.get("oemid"));
                ps.setString(2, estimateProportionMap.get("mediaid"));
                ps.setString(3, estimateProportionMap.get("adptype"));
                ps.setString(4, estimateProportionMap.get("daytime"));
                ps.setString(5, estimateProportionMap.get("moment"));
                ps.setString(6, estimateProportionMap.get("proportion"));
            }

            @Override
            public int getBatchSize() {
                return estimateProportionDataList.size();
            }
        });

        logger.info("记录预估占比数据[" + result.length + "]条!");
    }


    /**
     * 记录预估阈值数据
     *
     * @param estimateValueDataList
     */
    public void insertEstimateValueData(final List<Map<String, String>> estimateValueDataList) {

        int[] result = adSupersspJt.batchUpdate(insertEstimateValueSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, String> estimateValueMap = estimateValueDataList.get(i);
                ps.setString(1, estimateValueMap.get("projectid"));
                ps.setString(2, estimateValueMap.get("oemid"));
                ps.setString(3, estimateValueMap.get("mediaid"));
                ps.setString(4, estimateValueMap.get("creativeid"));
                ps.setString(5, estimateValueMap.get("adptype"));
                ps.setString(6, estimateValueMap.get("daytime"));
                ps.setString(7, estimateValueMap.get("moment"));
                ps.setString(8, estimateValueMap.get("esvalue"));
            }

            @Override
            public int getBatchSize() {
                return estimateValueDataList.size();
            }
        });

        logger.info("记录预估阈值数据[" + result.length + "]条!");
    }


    public float getHistoryWeight() {
        return historyWeight;
    }

    public void setHistoryWeight(float historyWeight) {
        this.historyWeight = historyWeight;
    }

    public float getSameTermWeight() {
        return sameTermWeight;
    }

    public void setSameTermWeight(float sameTermWeight) {
        this.sameTermWeight = sameTermWeight;
    }

    public float getYestodayWeight() {
        return yestodayWeight;
    }

    public void setYestodayWeight(float yestodayWeight) {
        this.yestodayWeight = yestodayWeight;
    }

    public String getGetProjectInfoSql() {
        return getProjectInfoSql;
    }

    public void setGetProjectInfoSql(String getProjectInfoSql) {
        this.getProjectInfoSql = getProjectInfoSql;
    }

    public String getGetRealProportionSql() {
        return getRealProportionSql;
    }

    public void setGetRealProportionSql(String getRealProportionSql) {
        this.getRealProportionSql = getRealProportionSql;
    }

    public String getGetEstimateProportionSql() {
        return getEstimateProportionSql;
    }

    public void setGetEstimateProportionSql(String getEstimateProportionSql) {
        this.getEstimateProportionSql = getEstimateProportionSql;
    }

    public String getInsertEstimateProportionSql() {
        return insertEstimateProportionSql;
    }

    public void setInsertEstimateProportionSql(String insertEstimateProportionSql) {
        this.insertEstimateProportionSql = insertEstimateProportionSql;
    }

    public String getInsertEstimateValueSql() {
        return insertEstimateValueSql;
    }

    public void setInsertEstimateValueSql(String insertEstimateValueSql) {
        this.insertEstimateValueSql = insertEstimateValueSql;
    }
}
