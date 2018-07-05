package com.voole.ad.prepare.detail;

import com.alibaba.fastjson.JSON;
import com.voole.ad.prepare.AbstractPrepareJob;
import com.voole.ad.utils.HttpClientUtil;
import com.voole.ad.utils.JedisUtil;
import com.voole.ad.utils.RedisClusterUtils;
import com.voole.ad.utils.SyncAliyunUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 *  实时计算统计天任务 redis集群模式
 * @author shaoyl 20180521
 */
public class RealTimeStatDay extends AbstractPrepareJob{
	

    private String toTimeFormat = "yyyy-MM-dd HH:mm:ss";//时间格式化
	
	@Autowired
	public RedisClusterUtils redisClusterUtils ;

    @Autowired
    public JdbcTemplate adSupersspJt;

    //是否往阿里云同步
    private  int syncFlag = 0;

    private SyncAliyunUtils syncAliyunUtils;




	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

//    @Override
    public void process_test(String time) {
        redisClusterUtils.set("redisCluster", "RedisClusterUtils");
        System.out.println(redisClusterUtils.get("redisCluster"));

    }

	@Override
	public void process(String time) {
		logger.info("--------------进入实时计算统计-------------------------");
        long startTime  = System.currentTimeMillis();



//        String parseTime = time;
        DateTime nowDt = new DateTime();
        String parseTime = nowDt.toString("yyyyMMdd");
        int minTime = Integer.valueOf(nowDt.toString("HHmm"));

        //解析时间
//        DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMdd");
//        DateTime parseDt  = DateTime.parse(time, localfromat);

        //入库时间戳
        String currStr = nowDt.toString("yyyy-MM-dd HH:mm:00");
        //凌晨跨天 头两分钟 特殊处理一次
        //0点时刻，再多处理一次昨日最后时刻数据
        if(minTime<2){
//            String toTime = dateTime.toString(toTimeFormat);
            DateTime yestodayDt =  nowDt.plusDays(-1);
            parseTime = yestodayDt.toString("yyyyMMdd");
            currStr = yestodayDt.toString("yyyy-MM-dd 23:59:59");
        }


//        从Redis获取天key值

        List<Map<String, String>> realTimeStatList = new ArrayList<Map<String, String>>();

        Set<String> adRtDayKeysSet = redisClusterUtils.smembers("adRtDayKeys");

        logger.info("获取到所有天的key为："+adRtDayKeysSet.toString());

        Iterator<String> it = adRtDayKeysSet.iterator() ;
        while(it.hasNext()){
            //key: daytime + creativeid + mediaid + oemid + adptypeid+te
            String  dayKeyTmp = it.next();


            logger.info("获取到 daykey = "+ dayKeyTmp);

            String[] dayValueArray = dayKeyTmp.split("_");

            if(dayValueArray.length<6){
                logger.warn("daykey="+dayKeyTmp+"不符合要求");
                continue;
            }

            String dayTime = dayValueArray[0];

            if(!dayTime.equals(parseTime)){

                logger.info("dayKey : "+dayKeyTmp + " 不需要处理" );

                continue;

            }

            logger.info(" -------- 开始解析 daykey = "+ dayKeyTmp +"的数据! ---------");

            //获取曝光数据
            String expDayKey = "expday_"+dayKeyTmp;
            String expDayValue = redisClusterUtils.get(expDayKey);

            String devDayKey = "devday_"+dayKeyTmp;
            long devDayValue = redisClusterUtils.scard(devDayKey);


            Map<String, String> realTimeStatMap = new HashMap<String, String>();

        //key: daytime + creativeid + mediaid + oemid + adptypeid + te

            realTimeStatMap.put("creativeid", dayValueArray[1]);
            realTimeStatMap.put("mediaid", dayValueArray[2]);
            realTimeStatMap.put("oemid", dayValueArray[3]);
            realTimeStatMap.put("adptype", dayValueArray[4]);
            realTimeStatMap.put("te", dayValueArray[5]);
            realTimeStatMap.put("playnum", expDayValue);
            realTimeStatMap.put("devnum", devDayValue+"");
            realTimeStatList.add(realTimeStatMap);


            logger.info("记录 daykey = "+dayKeyTmp +"的数数据为：" + realTimeStatMap.toString());

        }

        //入库处理
        insertRealTimeaStatData(realTimeStatList,currStr);


        //同步到阿里云
        if(syncFlag==1){

            logger.info("##### start sync real time data #####");

            String replaceWords = "stamp,last_update";

            syncAliyunUtils.syncRTDataToPlatform("ad_adptype_oemid_dayrt_v2","stamp",currStr,replaceWords);

            logger.info("##### end sync real time data #####");
        }

		long endTime = System.currentTimeMillis();
		logger.info("-------------结束实时计算统计,耗时("+(startTime-endTime)/1000+")秒！------------------------");
	}


    /**
     * 记录预估占比数据
     *
     * @param RealTimeStatResultList
     */
    public void insertRealTimeaStatData(final List<Map<String, String>> RealTimeStatResultList,final String currStr) {

        //获取当前时间
//        DateTime currTime = new DateTime();
//        final String currStr = currTime.toString("yyyy-MM-dd HH:mm:00");//(注：获取整点分钟时间)

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


    public int getSyncFlag() {
        return syncFlag;
    }

    public void setSyncFlag(int syncFlag) {
        this.syncFlag = syncFlag;
    }

    public SyncAliyunUtils getSyncAliyunUtils() {
        return syncAliyunUtils;
    }

    public void setSyncAliyunUtils(SyncAliyunUtils syncAliyunUtils) {
        this.syncAliyunUtils = syncAliyunUtils;
    }
}
