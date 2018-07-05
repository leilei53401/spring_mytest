package com.voole.ad.prepare.detail;

import com.voole.ad.prepare.AbstractPrepareJob;
import com.voole.ad.utils.RedisClusterUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * 实时计算删除天key任务 redis集群模式
 *
 * @author shaoyl 20180601
 */
public class RealTimeDelDayKey extends AbstractPrepareJob {


    @Autowired
    public RedisClusterUtils redisClusterUtils;


    private int keepSize; //保留天数

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }


    @Override
    public void process(String time) {
        logger.info("--------------进入实时计算删除天Key任务-------------------------");
        long startTime = System.currentTimeMillis();

        //解析时间
        DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime parseDt = DateTime.parse(time, localfromat);
        DateTime newParseDt = parseDt.plusDays(-keepSize);//保留指定天数据
        String parseTime = newParseDt.toString("yyyyMMdd");
        int keyParseTimeVal = Integer.valueOf(parseTime);

        logger.info("本次任务删除小于日期为["+parseTime+"]的数据");


//        从Redis获取天key值

        Set<String> adRtDayKeysSet = redisClusterUtils.smembers("adRtDayKeys");

        logger.info("获取到所有天的key为：" + adRtDayKeysSet.toString());

        Set<String> delDayKeysSet = new HashSet<String>();

        Iterator<String> it = adRtDayKeysSet.iterator();
        while (it.hasNext()) {
            //key: daytime+creativeid + oemid + adptypeid+te
            String dayKeyTmp = it.next();


            logger.info("获取到 daykey = " + dayKeyTmp);

            String[] dayValueArray = dayKeyTmp.split("_");

            String dayTime = dayValueArray[0];
            int keyDaytimeVal = Integer.valueOf(dayTime);


            if (keyDaytimeVal < keyParseTimeVal) {
                //删除小于的key
                logger.info(" -------- 开始删除 daykey = " + dayKeyTmp + " 的数据! ---------");
                //删除曝光key
                String expDayKey = "expday_" + dayKeyTmp;
                redisClusterUtils.del(expDayKey);
                //删除触达key
                String devDayKey = "devday_" + dayKeyTmp;
                redisClusterUtils.del(devDayKey);
                //记录要删除的key
                delDayKeysSet.add(dayKeyTmp);

                logger.info(" -------- 结束删除 daykey = " + dayKeyTmp + " 的数据! ---------");

            } else {
                logger.info(" --------  daykey = " + dayKeyTmp + " 的数据保留! ---------");
            }

        }

        logger.info("将要删除记录【"+delDayKeysSet.size()+"】条,具体信息为：" + delDayKeysSet.toString());

        //删除 set中的 key记录
        if (null != delDayKeysSet && delDayKeysSet.size() > 0) {
            String[] dayKeys = (String[]) delDayKeysSet.toArray(new String[delDayKeysSet.size()]);
            redisClusterUtils.srem("adRtDayKeys", dayKeys);
        }

        long endTime = System.currentTimeMillis();
        logger.info("-------------结束实时计算删除天key任务 耗时(" + (startTime - endTime) / 1000 + ")秒！------------------------");
    }


    @Override
    public void stop() {
        // TODO Auto-generated method stub
    }


    public int getKeepSize() {
        return keepSize;
    }

    public void setKeepSize(int keepSize) {
        this.keepSize = keepSize;
    }
}
