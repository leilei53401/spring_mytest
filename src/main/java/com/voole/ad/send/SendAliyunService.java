package com.voole.ad.send;

import java.util.List;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.voole.ad.utils.GlobalProperties;

/**
 * 离线数据发送阿里云同步
 * @author shaoyl
 */
@Service
public class SendAliyunService{
	private Logger logger = Logger.getLogger(SendAliyunService.class);	

	//是否需要休眠
	private int sleepflag = GlobalProperties.getInteger("sleepflag");//是否需要休眠
	//休眠时间
	private long sleeptime = GlobalProperties.getLong("sleeptime");//休眠时间毫秒
	

	@Resource
	private AdTimeSendServiceImpl adSendService;
	
	public SendAliyunService(){
	}
	
	
	/**
	 * 处理数据
	 * @param data
	 */
	public void doSend(List<String> paramList) {
	
			//处理一批数据
			for(String param:paramList){
		         adSendService.putLogToQueue(param);
			}
		
			//发送指定条数，休眠一段时间。
			if(1==sleepflag){
				try {
					Thread.sleep(sleeptime);
				} catch (InterruptedException e) {
					logger.error("休眠异常:",e);
				}
			}
		
	}

}
