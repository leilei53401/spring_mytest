package com.voole.ad.warning;


import com.voole.ad.utils.DateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.DoubleArray;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by shaoyl on 2018-04-03.
 * 发送第三方告警。
 * 获取当前半点发送数据与实时接收数据比较，超过预警占比则发送告警
 * 问题：发送 数据粒度只到创意 ， 告警维度到oemid 需商量。
 */
public class WarnSendThirdAlarmJob {

    private static Logger logger = Logger.getLogger(WarnSendThirdAlarmJob.class);

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


    String getProjectInfoSql; //获取项目计划信息查询语句

    private String getRealValueSql;//获取曝光具体值

    private String sendThirdValueSql;//获取发送第三方数据

    private String insertAlarmInfoSql;//记录告警


    //########################################################################

    public void sendAlarm() {
        //值考虑当前时间即可，其他时间告警也无意义。
        DateTime currDateStartTime = new DateTime();
        String todayStr = currDateStartTime.toString("yyyy-MM-dd");
        String todayStart = currDateStartTime.toString("yyyy-MM-dd 00:00:00");
        String todayTime = currDateStartTime.toString("yyyyMMdd");
        //获取当前半点时刻

     /*   String  currHourTimeStr = currDateStartTime.toString("yyyy-MM-dd HH");
        String  timeMinStr = currDateStartTime.toString("mm");

        DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime currHourTime = DateTime.parse(currHourTimeStr,fromat);

        DateTime endHalfTime  =  DateUtils.getEndHalf(currHourTime,Integer.valueOf(timeMinStr));


        String halfEndStr = endHalfTime.toString("yyyy-MM-dd HH:mm:ss");
        String halfEndMomentStr = endHalfTime.toString("yyyyMMddHHmmss");*/

        String halfEndStr = "";
        String halfEndMomentStr  = "";

        String  timeMinStr = currDateStartTime.toString("mm");
        if(Integer.valueOf(timeMinStr)>=30){
             halfEndStr =  currDateStartTime.toString("yyyy-MM-dd HH:30:00");
//             halfEndMomentStr =  currDateStartTime.toString("yyyyMMddHH3000");
        }else{
            halfEndStr = currDateStartTime.toString("yyyy-MM-dd HH:00:00");
//            halfEndMomentStr =  currDateStartTime.toString("yyyyMMddHH0000");

        }


        //for test
//        DateTime currDateStartTime = new DateTime(2018,1,5,17,50,30);
        //转化为整点10分钟时刻
//        DateTime tenEndTime = currDateStartTime.withMillis(DateUtils.getEndTen(currDateStartTime.getMillis(), 10));
//
//        String tenEndStr = tenEndTime.toString("yyyy-MM-dd HH:mm:ss");
//        String tenEndMomentStr = tenEndTime.toString("yyyyMMddHHmmss");



        Map<String, Map<String, Object>> projectPlanMap =  getProjectPlanMap(todayStr);


        if(null==projectPlanMap || projectPlanMap.size()==0){
            logger.warn("未获取到日期为[" + todayStr + "]的项目排期计划信息，本次任务结束!。");
            return;
        }

        Set<String> creativeSet = projectPlanMap.keySet();

        String creativeids = StringUtils.join(creativeSet,",");

        //###### 查询当前创意实际曝光量

        Map<String,String>  realValueMap = getRealValue(todayStart, halfEndStr, creativeids);

        //##### 查询当前创意预估阈值
        Map<String,String>  sendThirdValueMap = getSendThirdValue(todayStart, halfEndStr, creativeids);


        Iterator<String> creativesIt =   creativeSet.iterator();

        while(creativesIt.hasNext()){
            String creativeid = creativesIt.next();

            Map<String, Object> planInfoObj = projectPlanMap.get(creativeid);

            if(null==sendThirdValueMap.get(creativeid)){
                logger.warn("未获取到创意["+creativeid+"]发送第三方数据");
                continue;
            }
            double sendThirdValue = Double.valueOf(sendThirdValueMap.get(creativeid));

            if(sendThirdValue>0){

                double getRealTimeValue = Double.valueOf(realValueMap.get(creativeid));

                if(getRealTimeValue>0){

                    //#### 比较
                    double diffValue = sendThirdValue - getRealTimeValue;
                    int diffCpmValue = (int) diffValue / 1000;
                    double rate = diffValue / getRealTimeValue;
                    String alarmText = "";
                    int levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_CLEAR;
                    //################## 比较发送告警 #########################

                    //+++ 严重告警 +++
                    if (rate > 0 && Math.abs(rate) > thresholdEmergentRate) {
                        alarmText = "超出 " + (thresholdEmergentRate * 100) + "% 告警!";
                        levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_EMERGENT;

                    } else if (rate < 0 && Math.abs(rate) > thresholdEmergentRate) {
                        alarmText = "低于 " + (thresholdEmergentRate * 100) + "% 告警!";
                        levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_EMERGENT;

                    }
                    //+++ 重要告警 +++
                    else if (rate > 0 && Math.abs(rate) > thresholdImportantRate) {
                        alarmText = "超出 " + (thresholdImportantRate * 100) + "% 告警!";
                        levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_IMPORTANT;

                    } else if (rate < 0 && Math.abs(rate) > thresholdImportantRate) {
                        alarmText = "低于 " + (thresholdImportantRate * 100) + "% 告警!";
                        levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_IMPORTANT;

                    }
                    //+++ 一般告警 +++
                    else if (rate > 0 && rate > thresholdCommonRate) {
//                    logger.warn("###################### 超出 " + (thresholdCommonRate * 100) + "% 告警 ################");

                        alarmText = "超出 " + (thresholdCommonRate * 100) + "% 告警!";
                        levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_COMMON;

                    } else if (rate < 0 && Math.abs(rate) > thresholdCommonRate) {
//                    logger.warn("###################### 低于 " + (thresholdCommonRate * 100) + "% 告警 ################");

                        alarmText = "低于 " + (thresholdCommonRate * 100) + "% 告警!";
                        levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_COMMON;
                    } else {
                        logger.info("############### 时间为[" + halfEndStr + "]时刻正常！ ######################");
                        //清除告警或者没有告警(注：之前没有告警则不需要记录，有告警则需要清除。)

                        //STODO:清除告警(取消相同位置告警)
                        alarmText = "告警清除!";
                        levelid = WarnSendThirdAlarmJob.ALARM_LEVEL_CLEAR;
                    }

                    //记录告警数据
//                  String alarmContext = "项目[" + projectid + "]/渠道[" + oemid + "]/创意[" + creativeid + "]/相差[" + diffCpmValue + "] CPM ";



                    insertAlarmInfoData(todayTime, halfEndStr,  creativeid,  alarmText, diffCpmValue + "", rate + "", levelid , planInfoObj);


                }else{
                    logger.warn("未获取到创意【"+creativeid+"】的发送第三方数据！");
                }

            }else{
                logger.warn("未获取到创意【"+creativeid+"】的发送第三方数据！");
            }

        }

    }

    /**
     * 获取项目排期计划信息并转化成map形式
     * @注：一个创意对应多个 oemid
     * (TODO: 此处如何处理?monitor发送无法区分oemid)
     * @param todayStr
     * @return
     */
    private  Map<String, Map<String, Object>> getProjectPlanMap(String todayStr){

        String getProjectInfoSqlNew = getProjectInfoSql.replaceAll("@today", todayStr);

        Map<String, Map<String, Object>> projectPlanMap =  new HashMap<String,Map<String,Object>>();

        List<Map<String, Object>> projectPlanList = adSupersspJt.queryForList(getProjectInfoSqlNew);

        if (null != projectPlanList && projectPlanList.size() > 0) {

            logger.info("获取到时间为[" + todayStr + "]的有效项目计划数据【" + projectPlanList.size() + "】条!");

//            List<Map<String, String>> alarmInfoList = new ArrayList<Map<String, String>>();

            for (Map<String, Object> projectPlan : projectPlanList) {

//                String projectid = projectPlan.get("projectid").toString();
//                String scheduleid = projectPlan.get("scheduleid").toString();
//                String scheduleName = projectPlan.get("schedule_name").toString();
                  String creativeid = projectPlan.get("creativeid").toString();
//                String creativeName = projectPlan.get("creative_name").toString();
//                String oemid = projectPlan.get("oemid").toString();
//                String oemname = projectPlan.get("oemname").toString();
//                String adptype = projectPlan.get("adptype").toString();
//                String amount = projectPlan.get("amount").toString();//计划曝光量

                   projectPlanMap.put(creativeid,projectPlan);//注：相同创意会使用后一个计划。

            }

            logger.info("获取到转化后排期计划【"+projectPlanMap.size()+"】条！");
            logger.debug("获取转化后排期计划信息为： "+projectPlanMap.toString());
        } else {
            logger.warn("未获取到日期为[" + todayStr + "]的项目排期计划信息。");
        }

        return projectPlanMap;

    }


    /**
     *
     *  获取当前真实 接收数据情况
     * @param startTime
     * @param endTime
     * @return
     */
    private Map<String,String> getRealValue(String startTime, String endTime,  String creativeids) {

        Map<String,String>  realValueMap = new HashMap<String,String>();

        String getValueSql = getRealValueSql;
        getValueSql = getValueSql.replaceAll("@starttime", startTime).replaceAll("@endtime", endTime)
                .replaceAll("@creativeids", creativeids);

        logger.info("查询实际曝光sql为：/n" + getValueSql);

        List<Map<String, Object>> dayExpValueList = adSupersspJt.queryForList(getValueSql);

        if (null != dayExpValueList && dayExpValueList.size() > 0) {

            logger.info("获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "]的数据【" + dayExpValueList.size() + "】条!");

            for (Map<String, Object> data : dayExpValueList) {
                String creativeid = data.get("creativeid").toString();
                String playnum = data.get("playnum").toString();
                realValueMap.put(creativeid,playnum);
            }

            logger.info("获取到转化后开始时间为[" + startTime + "]，结束时间为[" + endTime + "]实时数据【"+realValueMap.size()+"】条!");

        } else {
            logger.warn("未获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "] 的实时数据");
        }
        return realValueMap;

    }

    /**
     * 获取发送第三方数据
     * @param dayStartStr
     * @param halfEndStr
     * @param creativeids
     * @return
     */
    private   Map<String,String> getSendThirdValue(String dayStartStr, String halfEndStr, String creativeids) {

        Map<String,String>  sendThirdValueMap = new HashMap<String,String>();

        String getValueSql = sendThirdValueSql;
        getValueSql = getValueSql.replaceAll("@starttime", dayStartStr).replaceAll("@endtime", halfEndStr)
                .replaceAll("@creativeids", creativeids);

        logger.info("预估查询sql为：/n" + getValueSql);

        List<Map<String, Object>> dayExpValueList = adSupersspJt.queryForList(getValueSql);

        if (null != dayExpValueList && dayExpValueList.size() > 0) {

            for (Map<String, Object> data : dayExpValueList) {
                String creativeid = data.get("creativeid").toString();
                String playnum = data.get("playnum").toString();
                sendThirdValueMap.put(creativeid,playnum);
            }

            logger.info("获取日期为时间为[" +  dayStartStr + "]，结束时间为[" + halfEndStr + "]数据【"+sendThirdValueMap.size()+"】条!");

        } else {
            logger.warn("未获取到获取日期为[" + dayStartStr + "]，结束时间为[" + halfEndStr + "]的发送第三方数据");
        }
        return sendThirdValueMap;

    }


    /**
     * 记录告警数据
     * @param daytime
     * @param endHalfTime
     * @param creativeid
     * @param alarmText
     * @param diffCpmValue
     * @param proportion
     * @param levelid
     * @param planInfoMap
     */
    public void insertAlarmInfoData(String daytime, String endHalfTime, final String creativeid,
                                     final String alarmText, final String diffCpmValue, final String proportion, final int levelid , Map<String,Object> planInfoMap) {

        String projectid = "";
        String scheduleid = "";
        String scheduleName = "";
        String creativeName = "";
        String oemid = "";
        String oemname = "";
        String adptype = "";
        try {
            projectid = planInfoMap.get("projectid").toString();
            scheduleid = planInfoMap.get("scheduleid").toString();
            scheduleName = planInfoMap.get("schedule_name").toString();

            creativeName = planInfoMap.get("creative_name").toString();
            oemid = planInfoMap.get("oemid").toString();
            oemname = planInfoMap.get("oemname").toString();
            adptype = planInfoMap.get("adptype").toString();
//            String amount = planInfoMap.get("amount").toString();
        } catch (Exception e) {
            logger.warn("获取创意【"+creativeid+"】计划排期信息错误：",e);
        }


        final  String alarmContext = "计划[" + scheduleName + "]/渠道[" + oemname + "]/创意[" + creativeName + "]/相差[" + diffCpmValue + "]CPM ";

        logger.warn("### 时间为[" + endHalfTime + "]时刻,发生" + alarmContext +", " + alarmText + "告警!");

        //查询是否存在今天的告警数据，如存在，则增加告警次数，不存在，则增加告警记录

        String selectSql = "select * from super_ssp_plan.monitor_alarm where alarmtype=1 and projectid='" + projectid + "' and scheduleid='"+scheduleid+"' and oemid='" + oemid + "'" +
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
                    " occurtime='" + currStr + "', rule_content='" + alarmContext + "', diffnum='"+diffCpmValue+"'" +
                    " where alarmtype=1 and projectid='" + projectid + "' and scheduleid='"+scheduleid+"' and oemid='" + oemid + "'" +
                    " and creativeid='" + creativeid + "' and ad_position='" + adptype + "'" +
                    " and DATE_FORMAT(fistoccurtime, '%Y%m%d')='" + daytime + "'";

            adSupersspJt.update(updateSql);


        } else {

            if (levelid > WarnSendThirdAlarmJob.ALARM_LEVEL_CLEAR) {

                //非清除告警类型才记录
                DateTime currTime = new DateTime();
                final String currStr = currTime.toString("yyyy-MM-dd HH:mm:ss");

                logger.warn("日期为[" + daytime + "]，projectid 为 [" + projectid + "], oemid=[" + oemid + "], creativeid=[" + creativeid + "], adptype=[" + adptype + "] 第一次告警");
                String insertSql = "insert into super_ssp_plan.monitor_alarm(projectid, scheduleid, oemid,  mediaid,  creativeid,  ad_position, " +
                        " area_code,  alarmtype,  rule_content,  diffnum,  " +
                        " send_status,  count,  level,  occurtime,  fistoccurtime, linkman)" +
                        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                final String mediaid = StringUtils.substring(oemid, 0, 6);


                final  String projectid_in = projectid;
                final String scheduleid_in = scheduleid;
                final String scheduleName_in = scheduleName;
                final String creativeName_in = creativeName;
                final String oemid_in = oemid;
                final String oemname_in = oemname;
                final String adptype_in = adptype;

                adSupersspJt.update(insertSql,
                        new PreparedStatementSetter() {

                            @Override
                            public void setValues(PreparedStatement ps) throws SQLException {
                                ps.setString(1, projectid_in);
                                ps.setString(2, scheduleid_in);
                                ps.setString(3, oemid_in);
                                ps.setString(4, mediaid);
                                ps.setString(5, creativeid);
                                ps.setString(6, adptype_in);
                                ps.setString(7, "-1");
                                ps.setString(8, "1");
                                ps.setString(9, alarmContext);
                                ps.setString(10, diffCpmValue);
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

    public String getSendThirdValueSql() {
        return sendThirdValueSql;
    }

    public void setSendThirdValueSql(String sendThirdValueSql) {
        this.sendThirdValueSql = sendThirdValueSql;
    }
}
