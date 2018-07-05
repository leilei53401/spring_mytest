package com.voole.ad.utils;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LimitedBlockingQueue<E> extends LinkedBlockingQueue<E> {
	
	public Logger logger = LoggerFactory.getLogger(LimitedBlockingQueue.class);
	public LimitedBlockingQueue(int maxSize) {
	    super(maxSize);
	}
	 /*
	  * (non-Javadoc)
	  * @see java.util.concurrent.LinkedBlockingQueue#offer(java.lang.Object)
	  * 线程池在将线程放入队列的过程中，调用的是offer方法，超出大小会返回false，从而引发reject异常
	  * 使用该队列，调用offer方法时，本质上是调用put方法，从而在队列满时，产生阻塞，从根本上解决线程reject异常
	  */
	  @Override
	  public boolean offer(E e) {
	    // turn offer() and add() into a blocking calls (unless interrupted)
	    try {
	    	put(e);
	    	return true;
	    } catch (InterruptedException ie) {
	    	Thread.currentThread().interrupt();
	    	logger.error("******put thread to limited blocking queue exception !******"+ie);
	    }
	    return false;
	  }
}
