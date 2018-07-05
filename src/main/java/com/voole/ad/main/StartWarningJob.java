package com.voole.ad.main;

import com.voole.ad.aggre.FlowOptAggreJob;
import com.voole.ad.extopt.detail.FlowOptOutPutJob;
import com.voole.ad.prepare.detail.FlowOptCalTerminalNumJob;
import com.voole.ad.warning.WarnCalRateJob;
import com.voole.ad.warning.WarnRealRateJob;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;
import java.util.Map;

/**
 * 实时预警后台计算
 */
public class StartWarningJob {
	
	private static Logger logger = Logger.getLogger(StartWarningJob.class);


    private WarnRealRateJob warnRealRateJob;// 计算实际占比任务

    private WarnCalRateJob warnCalRateJob;//计算预估占比和预估值任务

    private int timeDelay=-1;  //延时个数
    private int timeStepSize=1;//步长
    private int timeType=0;//0，天；1，小时

	/**
	 *  启动预警后台计算任务
	 * 有依赖关系，只能顺序执行
	 */
	private void start(){
		long start = System.currentTimeMillis();
        logger.info("--------------------进入预警后台计算任务---------------------");

        //默认按天处理
        DateTime currTime= new DateTime();
        DateTime startDateTime = currTime.plusDays(timeDelay);
        for(int i=0;i<timeStepSize;i++){
            DateTime createDateTime = startDateTime.plusDays(i);
            String dayValue = createDateTime.toString("yyyyMMdd");
            long dayStart = System.currentTimeMillis();
            logger.info("--------------------开始处理时间为【"+dayValue+"】的数据---------------------");
            execCalJob(dayValue);
            long dayEnd = System.currentTimeMillis();
            logger.info("--------------------结束处理时间为【"+dayValue+"】的数据,耗时["+(dayEnd-dayStart)/1000/60+"]分!---------------------");
        }

		long end = System.currentTimeMillis();
		logger.info("-------------------预警计算任务任务完成，耗时【"+(end-start)/1000/60+"】分!---------------------");
	}


    /**
     * 计算指定哪一天的数据
     * @param time
     */
    private void execCalJob(String time) {

        //计算实际占比数据
        if (null != warnRealRateJob) {
            logger.info("#################### 开始计算实际占比数据  #############################");
            warnRealRateJob.start();
            warnRealRateJob.process(time);
            warnRealRateJob.stop();
            logger.info("#################### 结束计算实际占比数据  #############################");
        }
        //执行计算预估占比数据
        if (null != warnCalRateJob) {
            logger.info("####################开始执行计算预估占比数据#############################");
            warnCalRateJob.start();
            warnCalRateJob.process(time);
            warnCalRateJob.stop();
            logger.info("####################结束执行计算预估占比数据#############################");
        }
    }


    public WarnRealRateJob getWarnRealRateJob() {
        return warnRealRateJob;
    }

    public void setWarnRealRateJob(WarnRealRateJob warnRealRateJob) {
        this.warnRealRateJob = warnRealRateJob;
    }

    public WarnCalRateJob getWarnCalRateJob() {
        return warnCalRateJob;
    }

    public void setWarnCalRateJob(WarnCalRateJob warnCalRateJob) {
        this.warnCalRateJob = warnCalRateJob;
    }

    public int getTimeDelay() {
        return timeDelay;
    }

    public void setTimeDelay(int timeDelay) {
        this.timeDelay = timeDelay;
    }

    public int getTimeStepSize() {
        return timeStepSize;
    }

    public void setTimeStepSize(int timeStepSize) {
        this.timeStepSize = timeStepSize;
    }

    public int getTimeType() {
        return timeType;
    }

    public void setTimeType(int timeType) {
        this.timeType = timeType;
    }
}
