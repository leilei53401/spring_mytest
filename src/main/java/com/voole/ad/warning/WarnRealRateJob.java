package com.voole.ad.warning;

import com.voole.ad.main.IJobLife;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by shaoyl on 2017-9-28.
 * 计算昨日所有项目实际占比数据
 * 循环每十分钟的点。
 * 每天计算一次
 */
public class WarnRealRateJob implements IJobLife {
    private static Logger logger = Logger.getLogger(WarnRealRateJob.class);

    @Autowired
    public JdbcTemplate adSupersspJt;

    private String getValidCreativeInPlan;

    private String getRealValueSql;//获取曝光具体值
    // 注：实时计算需要增加oemid维度的数据
   /*
   SELECT t.`mediaid` oemid,t.`creativeid`,t.`adptype`,MAX(playnum) playnum,MAX(devnum) devnum
    FROM `ad_report_adptype_dayrt` t
    WHERE t.`last_update` >= '2017-09-05 00:00:00' AND t.`last_update`<'2017-09-06 00:00:00'
    AND t.`creativeid`<>-1 AND t.`mediaid`<>-1 AND t.`adptype`<>-1
    AND te=0
    GROUP BY t.`mediaid`,t.`creativeid`,t.`adptype`
    */


    private String insertRealRateSql;

    @Override
    public void start() {

    }

    @Override
    public void process(String time) {

        DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime processDateTime = DateTime.parse(time, fromat);

        String startDayTime = processDateTime.toString("yyyy-MM-dd HH:mm:ss");

        DateTime endDateTime = processDateTime.plusDays(1);
        String endDayTime = endDateTime.toString("yyyy-MM-dd HH:mm:ss");
        String endDayValue = endDateTime.toString("yyyyMMddHHmmss");

        //获取昨天时间内有效排期内所有要计算数据的创意

        String validCreatives = getValidCreatives(startDayTime);

        logger.info("获取["+startDayTime+"]需计算创意列表为:["+validCreatives+"]");

        //获取天数据
        Map<String,String> dayValueMap = getValueMap(startDayTime,endDayTime,validCreatives);

        List<Map<String,String>> realRateList =  new ArrayList<Map<String,String>>();

        //每隔10分钟计算一次占比。
        DateTime startTenTime = processDateTime;
        while(startTenTime.isBefore(endDateTime.getMillis())){
//            String startTenStr =  startTenTime.toString("yyyy-MM-dd HH:mm:ss");

            DateTime endTentime = startTenTime.plusMinutes(10);
            String endTenStr =  endTentime.toString("yyyy-MM-dd HH:mm:ss");
            String endTenValue =  endTentime.toString("yyyyMMddHHmmss");

            Map<String,String> tenValueMap = getValueMap(startDayTime,endTenStr, validCreatives);

            //开始计算当前10分钟时刻占比
            Iterator<String> tenIt = tenValueMap.keySet().iterator();
            while (tenIt.hasNext()) {
                String tenKey = tenIt.next();
                String tenValueStr = tenValueMap.get(tenKey);
                float tenFloatValue = Float.valueOf(tenValueStr);
                //获取天对应数据
                String dayValueStr = dayValueMap.get(tenKey);

                float dayFloatValue = Float.valueOf(dayValueStr);
                //计算占比
                float tenRate = tenFloatValue/dayFloatValue;

                //解析key
                String[] keyArray = tenKey.split("_");

                //记录占比数据
                Map<String,String> realRateMap = new HashMap<String,String>();
                realRateMap.put("oemid",keyArray[0]);
                realRateMap.put("creativeid",keyArray[1]);
                realRateMap.put("mediaid", StringUtils.substring(keyArray[0],0,6));
                realRateMap.put("adptype",keyArray[2]);
                realRateMap.put("proportion",tenRate+"");
                realRateMap.put("daytime",time);
                realRateMap.put("moment",endTenValue);
                realRateList.add(realRateMap);
            }

            startTenTime = endTentime;
        } //every ten min while end

        //STODO:跨天0点时刻数据处理，保存昨天最后数据？占比为1.
        //注：不需要，十分钟循环里已包括
        /*Iterator<String> lastIt = dayValueMap.keySet().iterator();
        while (lastIt.hasNext()) {
            String lastKey = lastIt.next();
            String lastValueStr = dayValueMap.get(lastKey);

            //解析key
            String[] keyArray = lastKey.split("_");
            //记录占比数据
            Map<String,String> realRateMap = new HashMap<String,String>();
            realRateMap.put("oemid",keyArray[0]);
            realRateMap.put("creativeid",keyArray[1]);
            realRateMap.put("mediaid", StringUtils.substring(keyArray[0],0,6));
            realRateMap.put("adptype",keyArray[2]);
            realRateMap.put("daytime",time);
            realRateMap.put("moment",endDayValue);
            realRateMap.put("proportion","1");

            realRateList.add(realRateMap);
        }*/

        //记录真实占比表数据局
        insertRealRateData(realRateList);

    }

    @Override
    public void stop() {

    }


    private  String getValidCreatives(String toTime){
        String newSql = getValidCreativeInPlan.replaceAll("@yestoday", toTime);
        logger.info("preSql 替换后为："+newSql);
        List<String> creativeList=new ArrayList<String>();
        List<Map<String,Object>> creativeInfoList =  adSupersspJt.queryForList(newSql);
        for(Map<String,Object> creativeInfoMap:creativeInfoList) {
            String creativeId = creativeInfoMap.get("creativeid").toString();
            creativeList.add(creativeId);
        }
        return StringUtils.join(creativeList,",");
    }

    /**
     * 获取天的数据
     * @param startTime
     * @param endTime
     * @return
     */
    private Map<String,String> getValueMap(String startTime,String endTime,String creatives) {

//        DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyyMMdd");
//        DateTime processDateTime = DateTime.parse(time, fromat);
//        String startDayTime = processDateTime.toString("yyyy-MM-dd");
//
//        DateTime endDateTime = processDateTime.plusDays(1);
//        String endDayTime = endDateTime.toString("yyyy-MM-dd");


        //获取昨天总的曝光量,oemid_creativeid_adptype，字典缓存


        String getValueSql = getRealValueSql;
        getValueSql = getValueSql.replaceAll("@starttime", startTime);
        getValueSql = getValueSql.replaceAll("@endtime", endTime);
        getValueSql = getValueSql.replaceAll("@creatives", creatives);

        logger.info("替换后获取昨日实际数据sql为："+getValueSql);

        Map<String, String> dayValueMapCache = new HashMap<String, String>();

        List<Map<String, Object>> dayExpValueList = adSupersspJt.queryForList(getValueSql);

        if (null != dayExpValueList && dayExpValueList.size() > 0) {

            logger.info("获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "]的数据【" + dayExpValueList.size() + "】条!");

            for (Map<String, Object> mediaPlan : dayExpValueList) {

//                String oemid = mediaPlan.get("mediaid").toString();//!SFIXME: 要换成oemid。
                String oemid = mediaPlan.get("oemid").toString();
                String creativeid = mediaPlan.get("creativeid").toString();
                String adptype = mediaPlan.get("adptype").toString();
                String playnum = mediaPlan.get("playnum").toString();
                String devnum = mediaPlan.get("devnum").toString();
                String key = oemid + "_" + creativeid + "_" + adptype;

                dayValueMapCache.put(key, playnum);
            }

            logger.info("转化开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "]的数据【" + dayValueMapCache.size() + "】条!");

        } else {
            logger.warn("未获取到开始开始时间为[" + startTime + "]，结束时间为[" + endTime + "]的数据");
        }
        return dayValueMapCache;

    }


    /**
     *  记录前一天每十分钟实际占比计算结果
     * @param realRateDataList
     */
    public void insertRealRateData(final List<Map<String,String>> realRateDataList){

        int[] result = adSupersspJt.batchUpdate(insertRealRateSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String,String> realRateData = realRateDataList.get(i);
                ps.setString(1, realRateData.get("oemid"));
                ps.setString(2, realRateData.get("creativeid"));
                ps.setString(3, realRateData.get("mediaid"));
                ps.setString(4, realRateData.get("adptype"));
                ps.setString(5, realRateData.get("daytime"));
                ps.setString(6, realRateData.get("moment"));
                ps.setString(7, realRateData.get("proportion"));
            }

            @Override
            public int getBatchSize() {
                return realRateDataList.size();
            }
        });

        logger.info("记录实际占比数据["+result.length+"]条!");
    }

    public String getGetValidCreativeInPlan() {
        return getValidCreativeInPlan;
    }

    public void setGetValidCreativeInPlan(String getValidCreativeInPlan) {
        this.getValidCreativeInPlan = getValidCreativeInPlan;
    }

    public String getGetRealValueSql() {
        return getRealValueSql;
    }

    public void setGetRealValueSql(String getRealValueSql) {
        this.getRealValueSql = getRealValueSql;
    }

    public String getInsertRealRateSql() {
        return insertRealRateSql;
    }

    public void setInsertRealRateSql(String insertRealRateSql) {
        this.insertRealRateSql = insertRealRateSql;
    }
}
