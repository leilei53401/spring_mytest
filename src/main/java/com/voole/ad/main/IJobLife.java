package com.voole.ad.main;
/**
 * 任务生命周期
* @author shaoyl
* @date 2017-4-9 上午10:56:57 
* @version V1.0
 */
public interface IJobLife {
	public abstract void start();
	public abstract void process(String time);
	public abstract void stop();
}
