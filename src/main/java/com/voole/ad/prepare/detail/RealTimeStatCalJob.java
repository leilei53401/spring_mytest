package com.voole.ad.prepare.detail;

import com.voole.ad.prepare.AbstractPrepareJob;
import com.voole.ad.utils.JedisUtil;
import org.apache.commons.lang.StringUtils;
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
 *  实时计算统计天任务 基本模式测试
 * @author shaoyl 20180521
 */
public class RealTimeStatCalJob extends AbstractPrepareJob{


    private String toTimeFormat = "yyyy-MM-dd HH:mm:ss";//时间格式化



    @Autowired
    public JdbcTemplate adSupersspJt;


    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    //	@Override
    public void process(String time) {
        logger.info("--------------进入实时计算统计-------------------------");
        long startTime  = System.currentTimeMillis();
        DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMdd");
        //转化为 DateTime (注：TODO:凌晨跨天 头两分钟 特殊处理一次。)
        //0点时刻，再多处理一次昨日最后时刻数据
        DateTime dateTime  = DateTime.parse(time, localfromat);
        String toTime = dateTime.toString(toTimeFormat);


//        从Redis获取天key值

        List<Map<String, String>> realTimeStatList = new ArrayList<Map<String, String>>();

        Set<String> adRtDayKeysSet = JedisUtil.smembers("adRtDayKeys");

        Iterator<String> it = adRtDayKeysSet.iterator() ;
        while(it.hasNext()){
            //key: daytime+creativeid + oemid + adptypeid+te
            String  dayKeyTmp = it.next();


            logger.info("获取到 daykey = "+ dayKeyTmp);

            String[] dayValueArray = dayKeyTmp.split("_");

            String dayTime = dayValueArray[0];

            if(!dayTime.equals(time)){

                logger.info("dayKey : "+dayKeyTmp + " 不需要处理" );

                continue;

            }

            logger.info(" -------- 开始解析 daykey = "+ dayKeyTmp +"的数据! ---------");

            //获取曝光数据
            String expDayKey = "expday_"+dayKeyTmp;
            String expDayValue = JedisUtil.get(expDayKey);

            String devDayKey = "devday_"+dayKeyTmp;
            long devDayValue = JedisUtil.scard(devDayKey);


            Map<String, String> realTimeStatMap = new HashMap<String, String>();

            String oemid = dayValueArray[2];
            String mediaid = StringUtils.substring(oemid,0,6);

            realTimeStatMap.put("creativeid", dayValueArray[1]);
            realTimeStatMap.put("mediaid", mediaid);
            realTimeStatMap.put("oemid", oemid);
            realTimeStatMap.put("adptype", dayValueArray[3]);
            realTimeStatMap.put("te", dayValueArray[4]);
            realTimeStatMap.put("playnum", expDayValue);
            realTimeStatMap.put("devnum", devDayValue+"");
            realTimeStatList.add(realTimeStatMap);


            logger.info("记录 daykey = "+dayKeyTmp +"的数数据为：" + realTimeStatMap.toString());

        }


        //入库处理
        insertRealTimeaStatData(realTimeStatList);

        long endTime = System.currentTimeMillis();
        logger.info("-------------结束实时计算统计,耗时("+(startTime-endTime)/1000+")秒！------------------------");
    }



    /**
     * 记录预估占比数据
     *
     * @param RealTimeStatResultList
     */
    public void insertRealTimeaStatData(final List<Map<String, String>> RealTimeStatResultList) {

        //获取当前时间
        DateTime currTime = new DateTime();
        final String currStr = currTime.toString("yyyy-MM-dd HH:mm:00");//(注：获取整点分钟时间)

        String insertSql = "insert into super_ssp_report.ad_adptype_oemid_dayrt_v2(stamp, creativeid,   mediaid,  oemid,  adptype, " +
                " te,  playnum,  devnum,  last_update)" +
                " values(?,?,?,?,?,?,?,?,?)";

        int[] result = adSupersspJt.batchUpdate(insertSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, String> RealTimeStatResultMap = RealTimeStatResultList.get(i);
                ps.setString(1, currStr);
                ps.setString(2, RealTimeStatResultMap.get("creativeid"));
                ps.setString(3, RealTimeStatResultMap.get("mediaid"));
                ps.setString(4, RealTimeStatResultMap.get("oemid"));
                ps.setString(5, RealTimeStatResultMap.get("adptype"));
                ps.setString(6, RealTimeStatResultMap.get("te"));
                ps.setString(7, RealTimeStatResultMap.get("playnum"));
                ps.setString(8, RealTimeStatResultMap.get("devnum"));
                ps.setString(9, currStr);
            }

            @Override
            public int getBatchSize() {
                return RealTimeStatResultList.size();
            }
        });

        logger.info("记录实时统计数据[" + result.length + "]条!");
    }




    @Override
    public void stop() {
        // TODO Auto-generated method stub
    }



}
