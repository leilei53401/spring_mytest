package com.voole.ad.main;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * 离线计算任务入口类
 * 
 * @author shaoyl
 * @date 2017-4-9 上午10:56:33
 * @version V1.0
 */
public class StartJobPrePlan {

	private static Logger logger = Logger.getLogger(StartJobPrePlan.class);
	
	private IJobLife selectPreMac;
	private IJobLife selectPreDownloadMac;
	private IJobLife selecRrealtimeMac;
	private int timeDelay = -1; // 延时个数
	private int timeStepSize = 1;// 步长
	private int timeType = 0;// 0，天；1，小时
	/**
	 * 依次启动job 有依赖关系，只能顺序执行
	 */
	private void start() {
		
		long start = System.currentTimeMillis();
		
		
		System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa##########################");
		
		
		if (1 == timeType) {// 小时任务
			logger.info("--------------------处理小时类型数据，延时时间为【" + timeDelay + "】小时,步长为【" + timeStepSize
					+ "】小时的数据---------------------");
			DateTime currTime = new DateTime();
			DateTime startDateTime = currTime.plusHours(timeDelay);

			for (int i = 0; i < timeStepSize; i++) {
				DateTime createDateTime = startDateTime.plusHours(i);
				String hourValue = createDateTime.toString("yyyyMMddHH");
				long dayStart = System.currentTimeMillis();
				logger.info("--------------------开始处理时间为【" + hourValue + "】的数据---------------------");
				execStatJob(hourValue);
				long dayEnd = System.currentTimeMillis();
				logger.info("--------------------结束处理时间为【" + hourValue + "】的数据,耗时[" + (dayEnd - dayStart) / 1000 / 60
						+ "]分!---------------------");
			}

		} else {
			logger.info("--------------------处理天类型数据，延时时间为【" + timeDelay + "】天,步长为【" + timeStepSize
					+ "】天的数据---------------------");
			// 默认按天处理
			DateTime currTime = new DateTime();
			DateTime startDateTime = currTime.plusDays(timeDelay);
			for (int i = 0; i < timeStepSize; i++) {
				DateTime createDateTime = startDateTime.plusDays(i);
				String dayValue = createDateTime.toString("yyyyMMdd");
				long dayStart = System.currentTimeMillis();
				logger.info("--------------------开始处理时间为【" + dayValue + "】的数据---------------------");
				execStatJob(dayValue);
				long dayEnd = System.currentTimeMillis();
				logger.info("--------------------结束处理时间为【" + dayValue + "】的数据,耗时[" + (dayEnd - dayStart) / 1000 / 60
						+ "]分!---------------------");
			}
		}

		long end = System.currentTimeMillis();

		logger.info("-------------------任务处理完成，耗时【" + (end - start) / 1000 / 60 + "】分!---------------------");

	}

	private void execStatJob(String time) {
		//开机预下载挑选终端任务
		if (null != selectPreMac) {
			logger.info("####################开始执行挑选mac任务#############################");
			selectPreMac.start();
			selectPreMac.process(time);
			selectPreMac.stop();
		}
		if (null != selectPreDownloadMac) {
			logger.info("####################开始执行预下载汇报，补充Mac任务#############################");
			selectPreDownloadMac.start();
			selectPreDownloadMac.process(time); 
			selectPreDownloadMac.stop();
		}
		/*if (null != selecRrealtimeMac) {
			logger.info("####################开始执行实时调整，补充Mac任务#############################");
			selecRrealtimeMac.start();
			selecRrealtimeMac.process(time); 
			selecRrealtimeMac.stop();
		}*/
	}

	public static Logger getLogger() {
		return logger;
	}

	public static void setLogger(Logger logger) {
		StartJobPrePlan.logger = logger;
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

	public void setSelectPreMac(IJobLife selectPreMac) {
		this.selectPreMac = selectPreMac;
	}

	public void setSelectPreDownloadMac(IJobLife selectPreDownloadMac) {
		this.selectPreDownloadMac = selectPreDownloadMac;
	}

	public void setSelecRrealtimeMac(IJobLife selecRrealtimeMac) {
		this.selecRrealtimeMac = selecRrealtimeMac;
	}
	
}
