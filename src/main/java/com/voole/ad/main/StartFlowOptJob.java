package com.voole.ad.main;

import com.voole.ad.aggre.FlowOptAggreJob;
import com.voole.ad.extopt.detail.FlowOptOutPutJob;
import com.voole.ad.prepare.detail.FlowOptCalTerminalNumJob;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * 流量优化数据计算入口类
* @author shaoyl
* @date 2017-8-20 上午10:56:33
* @version V1.0
 */
public class StartFlowOptJob {
	
	private static Logger logger = Logger.getLogger(StartFlowOptJob.class);

    private FlowOptCalTerminalNumJob flowOptCalTerminalNumJob;

    private FlowOptAggreJob flowOptAggreJob;

    private FlowOptOutPutJob flowOptOutPutJob;

	/**
	 *  启动流量优化计算任务
	 * 有依赖关系，只能顺序执行
	 */
	private void start(){
		long start = System.currentTimeMillis();
        logger.info("--------------------进入流量优化计算任务---------------------");

        //###############  计算需要的终端个数 ###############
        Map<String,HashSet<String>> mediaReCalMap =  flowOptCalTerminalNumJob.calTerminalNums();

        logger.info("本次任务需要计算的媒体和渠道信息为：" + mediaReCalMap.toString());

        //test
   /*     HashSet<String> set = new HashSet<String>();
        set.add("900103101");
        Map<String,HashSet<String>> mediaReCalMap = new HashMap<String,HashSet<String>>();
        mediaReCalMap.put("900103",set);*/

       //计算有效终端并增加权重
        DateTime dt = new DateTime();
        String daytime = dt.toString("yyyyMMdd");

        DateTime yestoday = dt.plusDays(-1);
        String yestodayTime = yestoday.toString("yyyyMMdd");

        if(null!=flowOptAggreJob){
            //#############  执行终端计算任务 ##################
//            flowOptAggreJob.setMediaAndOemids(mediaReCalMap);
            flowOptAggreJob.process(yestodayTime, mediaReCalMap);
        }

        if(null!=flowOptOutPutJob){
            //#############  执行终端计算任务 ##################
//            flowOptOutPutJob.setMediaAndOemids(mediaReCalMap);
            flowOptOutPutJob.process(daytime, mediaReCalMap);
        }

		long end = System.currentTimeMillis();
		
		logger.info("-------------------流量优化计算任务任务完成，耗时【"+(end-start)/1000/60+"】分!---------------------");
		
	}


    public FlowOptCalTerminalNumJob getFlowOptCalTerminalNumJob() {
        return flowOptCalTerminalNumJob;
    }

    public void setFlowOptCalTerminalNumJob(FlowOptCalTerminalNumJob flowOptCalTerminalNumJob) {
        this.flowOptCalTerminalNumJob = flowOptCalTerminalNumJob;
    }

    public FlowOptAggreJob getFlowOptAggreJob() {
        return flowOptAggreJob;
    }

    public void setFlowOptAggreJob(FlowOptAggreJob flowOptAggreJob) {
        this.flowOptAggreJob = flowOptAggreJob;
    }

    public FlowOptOutPutJob getFlowOptOutPutJob() {
        return flowOptOutPutJob;
    }

    public void setFlowOptOutPutJob(FlowOptOutPutJob flowOptOutPutJob) {
        this.flowOptOutPutJob = flowOptOutPutJob;
    }



}
