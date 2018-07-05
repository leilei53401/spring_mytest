package com.voole.ad.send;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.ad.utils.GlobalProperties;

public class AdBlockQueue<T> {

	private Logger logger = LoggerFactory.getLogger(AdTimeOutQueue.class);
	private BlockingQueue<T> queue;
	private int max = GlobalProperties.getInteger("ad.block.queue.max");

	public AdBlockQueue() {
		queue = new ArrayBlockingQueue<T>(max);
	}
	
	
	/**
	 * 队列中添加数据
	 * 
	 * @param t
	 * @throws InterruptedException 
	 */
	public boolean putqueue(T t) throws InterruptedException {
		if (t == null) {
			logger.error("put queue exception, put data is null");
			return false;
		}
		queue.put(t);
		return true;
	}
	
	
	public T getData() throws InterruptedException {
		return queue.take();
	}
}
