package com.voole.ad.prepare.detail;

import com.voole.ad.prepare.AbstractPrepareJob;
import com.voole.ad.utils.RedisClusterUtils;
import com.voole.ad.utils.SyncAliyunUtils;
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
 *  实时计算统计天任务 redis集群模式
 * @author shaoyl 20180521
 */
public class RealTimeStatHour extends AbstractPrepareJob{
	

    private String toTimeFormat = "yyyy-MM-dd HH:mm:ss";//时间格式化
	
	@Autowired
	public RedisClusterUtils redisClusterUtils ;

    @Autowired
    public JdbcTemplate adSupersspJt;

    //是否重新计算任务
    private int reCal=0;

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

        logger.info("--------------进入实时小时计算统计-------------------------");
        long startTime  = System.currentTimeMillis();

        //跨小时整点，或者小于整点开始两分钟内时间，对上一个小时最后统计一次。
        //即整点时刻再更新一次上一个小时的数据
        //当前时间
        String parseTime = time;
        DateTime nowDt = new DateTime();
        int minTime = Integer.valueOf(nowDt.toString("mm"));
        //解析时间
        DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMddHH");
        DateTime parseDt  = DateTime.parse(time, localfromat);

        if(reCal==0 && minTime<2){
//            String toTime = dateTime.toString(toTimeFormat);
            parseDt =  parseDt.plusHours(-1);
            parseTime = parseDt.toString("yyyyMMddHH");
        }


        String parseHour = parseDt.toString("yyyy-MM-dd HH:00:00");
        String currStr = nowDt.toString("yyyy-MM-dd HH:mm:ss");


//        从Redis获取天key值

        List<Map<String, String>> realTimeStatList = new ArrayList<Map<String, String>>();

        Set<String> adRtHourKeysSet = redisClusterUtils.smembers("adRtHourKeys");


        logger.info("获取到所有天的key为："+adRtHourKeysSet.toString());

        Iterator<String> it = adRtHourKeysSet.iterator() ;
        while(it.hasNext()){
            //key: daytime+  creativeid + mediaid + oemid + adptypeid+te
            String  hourKeyTmp = it.next();


            logger.info("获取到 hourkey = "+ hourKeyTmp);

            String[] hourValueArray = hourKeyTmp.split("_");

            if(hourValueArray.length<6){
                logger.warn("hourkey="+hourKeyTmp+"不符合要求");
                continue;
            }

            String hourTime = hourValueArray[0];

            if(!hourTime.equals(parseTime)){

                logger.info("hourkey : "+hourKeyTmp + " 不需要处理" );

                continue;

            }

            logger.info(" -------- 开始解析 hourkey = "+ hourKeyTmp +"的数据! ---------");

            //获取曝光数据
            String expHourKey = "exphour_"+hourKeyTmp;
            String expHourValue = redisClusterUtils.get(expHourKey);

            String devHourKey = "devhour_"+hourKeyTmp;
            long devHourValue = redisClusterUtils.scard(devHourKey);


            Map<String, String> realTimeStatMap = new HashMap<String, String>();

            //key: daytime+  creativeid + mediaid + oemid + adptypeid+te

            realTimeStatMap.put("creativeid", hourValueArray[1]);
            realTimeStatMap.put("mediaid", hourValueArray[2]);
            realTimeStatMap.put("oemid", hourValueArray[3]);
            realTimeStatMap.put("adptype", hourValueArray[4]);
            realTimeStatMap.put("te", hourValueArray[5]);
            realTimeStatMap.put("playnum", expHourValue);
            realTimeStatMap.put("devnum", devHourValue+"");
            realTimeStatList.add(realTimeStatMap);


            logger.info("记录 hourkey = "+hourKeyTmp +"的数数据为：" + realTimeStatMap.toString());

        }


        //入库处理
        insertRealTimeaStatData(realTimeStatList,parseHour,currStr);


        //同步到阿里云
        if(syncFlag==1){

            logger.info("##### start sync real time data #####");

            String replaceWords = "stamp,last_update";

            syncAliyunUtils.syncRTDataToPlatform("ad_adptype_oemid_hourrt","stamp",parseHour,replaceWords);

            logger.info("##### end sync real time data #####");
        }

		long endTime = System.currentTimeMillis();
		logger.info("-------------结束实时小时计算统计,耗时("+(startTime-endTime)/1000+")秒！------------------------");
	}



    /**
     * 记录小时实时数据
     *
     * @param RealTimeStatResultList
     */
    public void insertRealTimeaStatData(final List<Map<String, String>> RealTimeStatResultList,final String parseHour,final String currStr) {


        String insertSql = "REPLACE INTO super_ssp_report.ad_adptype_oemid_hourrt(stamp, creativeid,  mediaid,  oemid,  adptype, " +
                " te,  playnum,  devnum,  last_update)" +
                " values(?,?,?,?,?,?,?,?,?)";

        int[] result = adSupersspJt.batchUpdate(insertSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, String> RealTimeStatResultMap = RealTimeStatResultList.get(i);
                ps.setString(1, parseHour);
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

        logger.info("记录实时统计小时数据[" + result.length + "]条!");
    }

	


	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

    public int getReCal() {
        return reCal;
    }

    public void setReCal(int reCal) {
        this.reCal = reCal;
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
