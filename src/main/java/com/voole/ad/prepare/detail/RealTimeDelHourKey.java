package com.voole.ad.prepare.detail;

import com.voole.ad.prepare.AbstractPrepareJob;
import com.voole.ad.utils.RedisClusterUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * 实时计算删除小时key任务 redis集群模式
 *
 * @author shaoyl 20180601
 */
public class RealTimeDelHourKey extends AbstractPrepareJob {


    @Autowired
    public RedisClusterUtils redisClusterUtils;


    private int keepSize; //保留小时

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }


    @Override
    public void process(String time) {
        logger.info("--------------进入实时计算删除小时Key任务-------------------------");
        long startTime = System.currentTimeMillis();

        //解析时间
        DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMddHH");
        DateTime parseDt = DateTime.parse(time, localfromat);
        DateTime newParseDt = parseDt.plusHours(-keepSize);//保留指定小时数据
        String parseTime = newParseDt.toString("yyyyMMddHH");
        int keyParseTimeVal = Integer.valueOf(parseTime);

        logger.info("本次任务删除小于时间为["+parseTime+"]的数据");

        Set<String> adRtHourKeysSet = redisClusterUtils.smembers("adRtHourKeys");

        logger.info("获取到所有小时的key为：" + adRtHourKeysSet.toString());

        Set<String> delHourKeysSet = new HashSet<String>();

        Iterator<String> it = adRtHourKeysSet.iterator();
        while (it.hasNext()) {
            //key: hourtime+creativeid + oemid + adptypeid+te
            String hourKeyTmp = it.next();


            logger.info("获取到 hourkey = " + hourKeyTmp);

            String[] hourValueArray = hourKeyTmp.split("_");

            String hourTime = hourValueArray[0];
            int keyHourtimeVal = Integer.valueOf(hourTime);


            if (keyHourtimeVal < keyParseTimeVal) {
                //删除小于的key
                logger.info(" -------- 开始删除 hourkey = " + hourKeyTmp + " 的数据! ---------");
                //删除曝光key
                String expHourKey = "exphour_" + hourKeyTmp;
                redisClusterUtils.del(expHourKey);
                //删除触达key
                String devHourKey = "devhour_" + hourKeyTmp;
                redisClusterUtils.del(devHourKey);
                //记录要删除的key
                delHourKeysSet.add(hourKeyTmp);

                logger.info(" -------- 结束删除 hourkey = " + hourKeyTmp + " 的数据! ---------");

            } else {
                logger.info(" --------  hourkey = " + hourKeyTmp + " 的数据保留! ---------");
            }

        }

        logger.info("将要删除记录【"+delHourKeysSet.size()+"】条,具体信息为：" + delHourKeysSet.toString());

        //删除 set中的 key记录
        if (null != delHourKeysSet && delHourKeysSet.size() > 0) {
            String[] hourKeys = (String[]) delHourKeysSet.toArray(new String[delHourKeysSet.size()]);
            redisClusterUtils.srem("adRtHourKeys", hourKeys);
        }

        long endTime = System.currentTimeMillis();
        logger.info("-------------结束实时计算删除小时key任务 耗时(" + (startTime - endTime) / 1000 + ")秒！------------------------");
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
