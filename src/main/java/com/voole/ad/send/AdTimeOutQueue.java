package com.voole.ad.send;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.ad.utils.GlobalProperties;

/**
 * @author Administrator
 *
 */
public class AdTimeOutQueue<T> {

	private Logger logger = LoggerFactory.getLogger(AdTimeOutQueue.class);
	private BlockingQueue<T> queue;
	private int max = GlobalProperties.getInteger("ad.timeout.queue.max");
	private int clearcnt = GlobalProperties.getInteger("ad.timeout.queue.clearcnt");

	public AdTimeOutQueue() {
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
		this.checkQueueFull();
		queue.put(t);
		return true;
	}

	/**
	 * 检查队列是否达到最大值,达到上限则清除一部分数据
	 * 
	 * @return
	 */
	public synchronized void checkQueueFull() {
		if (queue.size() > max) {
			//记文件或者记录达到上限日志记录
			logger.warn("queue reach up bound,playlog is cleared,clear count=" + clearcnt);
			for (int i = 0; i < clearcnt; i++) {
				logger.warn("clear playlog is ==========" + queue.poll());
			}
		}

	}

	/**
	 * 从队列获取数据
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public T getData() throws InterruptedException {
		return queue.take();
	}
}
