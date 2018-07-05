package com.voole.ad.warning;


import com.voole.ad.utils.DateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by shaoyl on 2017-9-28.
 * 告警计算并发送。
 * 获取当前十分钟接收实际数据并和预估数据做比较，超过预警值则发送告警
 */
public class WarnSendJob {

    private static Logger logger = Logger.getLogger(WarnSendJob.class);

    public static final int ALARM_LEVEL_CLEAR = 0;
    public static final int ALARM_LEVEL_COMMON = 1;
    public static final int ALARM_LEVEL_IMPORTANT = 2;
    public static final int ALARM_LEVEL_EMERGENT = 3;

    @Autowired
    public JdbcTemplate adSupersspJt;

    private double thresholdCommonRate = 0.05;
    private double thresholdImportantRate = 0.1;
    private double thresholdEmergentRate = 0.15;

    private String linkman = "1,4";


    String getProjectInfoSql;

    private String getRealValueSql;//获取曝光具体值

    private String getEstimateValueSql;//获取预估阈值

    private String insertAlarmInfoSql;//记录告警


    //########################################################################


    public void sendAlarm() {
        //值考虑当前时间即可，其他时间告警也无意义。
        DateTime currDateStartTime = new DateTime();
        //for test
//        DateTime currDateStartTime = new DateTime(2018,1,5,17,50,30);
        //转化为整点10分钟时刻
        DateTime tenEndTime = currDateStartTime.withMillis(DateUtils.getEndTen(currDateStartTime.getMillis(), 10));

        String todayStr = currDateStartTime.toString("yyyy-MM-dd");
        String todayStart = currDateStartTime.toString("yyyy-MM-dd 00:00:00");
        String todayTime = currDateStartTime.toString("yyyyMMdd");
        String tenEndStr = tenEndTime.toString("yyyy-MM-dd HH:mm:ss");
        String tenEndMomentStr = tenEndTime.toString("yyyyMMddHHmmss");

        String getProjectInfoSqlNew = getProjectInfoSql.replaceAll("@today", todayStr);


        List<Map<String, Object>> projectPlanList = adSupersspJt.queryForList(getProjectInfoSqlNew);

        if (null != projectPlanList && projectPlanList.size() > 0) {

            logger.warn("获取到时间为[" + todayStr + "]的有效项目计划数据【" + projectPlanList.size() + "】条!");

//            List<Map<String, String>> alarmInfoList = new ArrayList<Map<String, String>>();

            for (Map<String, Object> projectPlan : projectPlanList) {

                String projectid = projectPlan.get("projectid").toString();
                String scheduleid = projectPlan.get("scheduleid").toString();
                String scheduleName = projectPlan.get("schedule_name").toString();
                String creativeid = projectPlan.get("creativeid").toString();
                String creativeName = projectPlan.get("creative_name").toString();
                String oemid = projectPlan.get("oemid").toString();
                String oemname = projectPlan.get("oemname").toString();
                String adptype = projectPlan.get("adptype").toString();
                String amount = projectPlan.get("amount").toString();//计划曝光量

                //###### 查询当前创意实际曝光量

                double realExp = getRealValue(todayStart, tenEndStr, oemid, creativeid, adptype);

                //##### 查询当前创意预估阈值
                double estimateExp = getEstimateValue(todayTime, tenEndMomentStr, oemid, creativeid, adptype);


                if(estimateExp>0) {

                    //#### 比较
                    double diffValue = realExp - estimateExp;
                    int diffCpmValue = (int) diffValue / 1000;
                    double rate = diffValue / estimateExp;
                    String alarmText = "";
                    int levelid = WarnSendJob.ALARM_LEVEL_CLEAR;
                    //################## 比较发送告警 #########################

                    //+++ 严重告警 +++
                    if (rate > 0 && Math.abs(rate) > thresholdEmergentRate) {
                        alarmText = "超出 " + (thresholdEmergentRate * 100) + "% 告警!";
                        levelid = WarnSendJob.ALARM_LEVEL_EMERGENT;

                    } else if (rate < 0 && Math.abs(rate) > thresholdEmergentRate) {
                        alarmText = "低于 " + (thresholdEmergentRate * 100) + "% 告警!";
                        levelid = WarnSendJob.ALARM_LEVEL_EMERGENT;

                    }
                    //+++ 重要告警 +++
                    else if (rate > 0 && Math.abs(rate) > thresholdImportantRate) {
                        alarmText = "超出 " + (thresholdImportantRate * 100) + "% 告警!";
                        levelid = WarnSendJob.ALARM_LEVEL_IMPORTANT;

                    } else if (rate < 0 && Math.abs(rate) > thresholdImportantRate) {
                        alarmText = "低于 " + (thresholdImportantRate * 100) + "% 告警!";
                        levelid = WarnSendJob.ALARM_LEVEL_IMPORTANT;

                    }
                    //+++ 一般告警 +++
                    else if (rate > 0 && rate > thresholdCommonRate) {
//                    logger.warn("###################### 超出 " + (thresholdCommonRate * 100) + "% 告警 ################");

                        alarmText = "超出 " + (thresholdCommonRate * 100) + "% 告警!";
                        levelid = WarnSendJob.ALARM_LEVEL_COMMON;

                    } else if (rate < 0 && Math.abs(rate) > thresholdCommonRate) {
//                    logger.warn("###################### 低于 " + (thresholdCommonRate * 100) + "% 告警 ################");

                        alarmText = "低于 " + (thresholdCommonRate * 100) + "% 告警!";
                        levelid = WarnSendJob.ALARM_LEVEL_COMMON;
                    } else {
                        logger.info("############### 时间为[" + tenEndStr + "]时刻正常！ ######################");
                        //清除告警或者没有告警(注：之前没有告警则不需要记录，有告警则需要清除。)

                        //STODO:清除告警(取消相同位置告警)
                        alarmText = "告警清除!";
                        levelid = WarnSendJob.ALARM_LEVEL_CLEAR;
                    }

                    //记录告警数据
//                  String alarmContext = "项目[" + projectid + "]/渠道[" + oemid + "]/创意[" + creativeid + "]/相差[" + diffCpmValue + "] CPM ";

                    String alarmContext = "计划[" + scheduleName + "]/渠道[" + oemname + "]/创意[" + creativeName + "]/相差[" + diffCpmValue + "]CPM ";

                    logger.warn("### 时间为[" + tenEndStr + "]时刻,发生" + alarmContext + alarmText + "告警!");

                    insertAlarmInfoData(todayTime, projectid, scheduleid,oemid, creativeid, adptype, alarmContext, diffCpmValue + "", rate + "", levelid);
                }else{
                    //未获取到预估数据
                    logger.warn("### 未获取到oemid为["+oemid+"],创意为["+creativeid+"],时间为[" + tenEndStr + "]的预估数据!");
                }

            }

        } else {
            logger.warn("未获取到日期为[" + todayStr + "]的项目排期计划信息。");
        }

    }


    /**
     * 获取天的数据
     *
     * @param startTime
     * @param endTime
     * @return
     */
    private double getRealValue(String startTime, String endTime, String oemid, String creativeid, String adptype) {

        double result = 0;

        //获取昨天总的曝光量,oemid_creativeid_adptype，字典缓存

        //STODO: 目前实时表中数据没有oemid维度，只有到媒体 维度，需要增加任务或者改造原实时报表结构

        String getValueSql = getRealValueSql;
        getValueSql = getValueSql.replaceAll("@starttime", startTime).replaceAll("@endtime", endTime)
                .replaceAll("@oemid", oemid).replaceAll("@creativeid", creativeid).replaceAll("@adptype", adptype);
        logger.info("查询实际曝光sql为：/n" + getValueSql);
        List<Map<String, Object>> dayExpValueList = adSupersspJt.queryForList(getValueSql);

        if (null != dayExpValueList && dayExpValueList.size() > 0) {

            logger.info("获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "]的数据【" + dayExpValueList.size() + "】条!");

            for (Map<String, Object> mediaPlan : dayExpValueList) {
                String playnum = mediaPlan.get("playnum").toString();
                String devnum = mediaPlan.get("devnum").toString();
                result = Double.valueOf(playnum);
            }

            logger.info("转化开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "], oemid=[" + oemid + "], creativeid=[" + creativeid + "], adptype=[" + adptype + "] 的数据为【" + result + "】!");

        } else {
            logger.warn("未获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "] oemid=[" + oemid + "], creativeid=[" + creativeid + "], adptype=[" + adptype + "] 的数据");
        }
        return result;

    }

    /**
     * 获取预估值
     *
     * @param daytime
     * @param moment
     * @param oemid
     * @param creativeid
     * @param adptype
     * @return
     */
    private double getEstimateValue(String daytime, String moment, String oemid, String creativeid, String adptype) {

        double result = 0;

        //获取昨天总的曝光量,oemid_creativeid_adptype，字典缓存

        //STODO: 目前实时表中数据没有oemid维度，只有到媒体 维度，需要增加任务或者改造原实时报表结构

        String getValueSql = getEstimateValueSql;
        getValueSql = getValueSql.replaceAll("@daytime", daytime).replaceAll("@moment", moment)
                .replaceAll("@oemid", oemid).replaceAll("@creativeid", creativeid).replaceAll("@adptype", adptype);

        logger.info("预估查询sql为：/n" + getValueSql);

        List<Map<String, Object>> dayExpValueList = adSupersspJt.queryForList(getValueSql);

        if (null != dayExpValueList && dayExpValueList.size() > 0) {

            for (Map<String, Object> mediaPlan : dayExpValueList) {
                String esvalue = mediaPlan.get("esvalue").toString();
//                String devnum = mediaPlan.get("devnum").toString();
                result = Double.valueOf(esvalue);
            }

            logger.info("获取日期为时间为[" + daytime + "]，时间为[" + moment + "], oemid=[" + oemid + "], creativeid=[" + creativeid + "], adptype=[" + adptype + "] 的预估数据为【" + result + "】!");

        } else {
            logger.warn("未获取到获取日期为[" + daytime + "]，时间为[" + moment + "], oemid=[" + oemid + "], creativeid=[" + creativeid + "], adptype=[" + adptype + "] 的预估数据");
        }
        return result;

    }


    /**
     * 记录告警数据
     *
     * @param daytime
     * @param projectid
     * @param oemid
     * @param creativeid
     * @param adptype
     * @param alarmContext
     * @param value
     * @param proportion
     * @param levelid
     */
    public void insertAlarmInfoData(String daytime, final String projectid, final String scheduleid,final String oemid, final String creativeid,
                                    final String adptype, final String alarmContext, final String value, final String proportion, final int levelid) {

        //查询是否存在今天的告警数据，如存在，则增加告警次数，不存在，则增加告警记录

        String selectSql = "select * from super_ssp_plan.monitor_alarm where alarmtype=0 and projectid='" + projectid + "' and scheduleid='"+scheduleid+"' and oemid='" + oemid + "'" +
                " and creativeid='" + creativeid + "' and ad_position='" + adptype + "'" +
                " and DATE_FORMAT(fistoccurtime, '%Y%m%d')='" + daytime + "'";

        List<Map<String, Object>> alarmList = adSupersspJt.queryForList(selectSql);

        if (null != alarmList && alarmList.size() > 0) {
            DateTime currTime = new DateTime();
            String currStr = currTime.toString("yyyy-MM-dd HH:mm:ss");
            //告警存在 , 更新
            logger.info("日期为[" + daytime + "]， projectid 为[" + projectid + "], scheduleid=["+scheduleid+"] oemid=[" + oemid + "], creativeid=[" + creativeid + "], adptype=[" + adptype + "] 存在告警!");

//            String count = alarmList.get(0).get("count").toString();
            //更新，并增加告警次数
            String updateSql = "update super_ssp_plan.monitor_alarm set level=" + levelid + " , count=count+1 , " +
                    " occurtime='" + currStr + "', rule_content='" + alarmContext + "', diffnum='"+value+"'" +
                    " where alarmtype=0 and projectid='" + projectid + "' and scheduleid='"+scheduleid+"' and oemid='" + oemid + "'" +
                    " and creativeid='" + creativeid + "' and ad_position='" + adptype + "'" +
                    " and DATE_FORMAT(fistoccurtime, '%Y%m%d')='" + daytime + "'";

            adSupersspJt.update(updateSql);


        } else {

            if (levelid > WarnSendJob.ALARM_LEVEL_CLEAR) {

                //非清除告警类型才记录
                DateTime currTime = new DateTime();
                final String currStr = currTime.toString("yyyy-MM-dd HH:mm:ss");

                logger.warn("日期为[" + daytime + "]，projectid 为 [" + projectid + "], oemid=[" + oemid + "], creativeid=[" + creativeid + "], adptype=[" + adptype + "] 第一次告警");
                String insertSql = "insert into super_ssp_plan.monitor_alarm(projectid, scheduleid, oemid,  mediaid,  creativeid,  ad_position, " +
                        " area_code,  alarmtype,  rule_content,  diffnum,  " +
                        " send_status,  count,  level,  occurtime,  fistoccurtime, linkman)" +
                        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                final String mediaid = StringUtils.substring(oemid, 0, 6);

                adSupersspJt.update(insertSql,
                        new PreparedStatementSetter() {
                            @Override
                            public void setValues(PreparedStatement ps) throws SQLException {
                                ps.setString(1, projectid);
                                ps.setString(2, scheduleid);
                                ps.setString(3, oemid);
                                ps.setString(4, mediaid);
                                ps.setString(5, creativeid);
                                ps.setString(6, adptype);
                                ps.setString(7, "-1");
                                ps.setString(8, "0");
                                ps.setString(9, alarmContext);
                                ps.setString(10, value);
                                ps.setString(11, "0");
                                ps.setString(12, "1");
                                ps.setString(13, levelid + "");
                                ps.setString(14, currStr);
                                ps.setString(15, currStr);
                                ps.setString(16, linkman);//linkman
                            }
                        });
            }

        }

        logger.info("记录告警数据结束!");
    }


    public double getThresholdCommonRate() {
        return thresholdCommonRate;
    }

    public void setThresholdCommonRate(double thresholdCommonRate) {
        this.thresholdCommonRate = thresholdCommonRate;
    }

    public double getThresholdImportantRate() {
        return thresholdImportantRate;
    }

    public void setThresholdImportantRate(double thresholdImportantRate) {
        this.thresholdImportantRate = thresholdImportantRate;
    }

    public double getThresholdEmergentRate() {
        return thresholdEmergentRate;
    }

    public void setThresholdEmergentRate(double thresholdEmergentRate) {
        this.thresholdEmergentRate = thresholdEmergentRate;
    }

    public String getGetProjectInfoSql() {
        return getProjectInfoSql;
    }

    public void setGetProjectInfoSql(String getProjectInfoSql) {
        this.getProjectInfoSql = getProjectInfoSql;
    }

    public String getGetRealValueSql() {
        return getRealValueSql;
    }

    public void setGetRealValueSql(String getRealValueSql) {
        this.getRealValueSql = getRealValueSql;
    }

    public String getGetEstimateValueSql() {
        return getEstimateValueSql;
    }

    public void setGetEstimateValueSql(String getEstimateValueSql) {
        this.getEstimateValueSql = getEstimateValueSql;
    }

    public String getInsertAlarmInfoSql() {
        return insertAlarmInfoSql;
    }

    public void setInsertAlarmInfoSql(String insertAlarmInfoSql) {
        this.insertAlarmInfoSql = insertAlarmInfoSql;
    }

    public String getLinkman() {
        return linkman;
    }

    public void setLinkman(String linkman) {
        this.linkman = linkman;
    }
}
