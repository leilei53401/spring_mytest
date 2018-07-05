package com.voole.ad.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程池
 * 
 * @author Administrator
 * @version 1.0
 */
public class ThreadPool {

	private ThreadPoolExecutor threadPool;

	public ThreadPool(){

	}

	/**
	 * 
	 * @param queueSize
	 * @param keepAliveTime
	 * @param maxPoolSize
	 * @param corePoolSize
	 */
	public ThreadPool( int corePoolSize, int maxPoolSize, int queueSize, int keepAliveTime){
		initThreadPool(queueSize, keepAliveTime, maxPoolSize, corePoolSize);
	}

	/**
	 * 
	 * @param command
	 */
	public void execute(Runnable command){
		threadPool.execute(command);
	}

	public ThreadPoolExecutor getThreadPool(){
		return threadPool;
	}

	/**
	 * 初始化线程池
	 * 
	 * @param queueSize  线程池队列大小
	 * @param keepAliveTime 存活时间
	 * @param maxPoolSize  线程池最大线程数
	 * @param corePoolSize  核心线程数
	 */
	public void initThreadPool(int queueSize, int keepAliveTime, int maxPoolSize, int corePoolSize){
		if(threadPool == null){
			threadPool = new  ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),	new DefaultThreadFactory(),	new ThreadPoolExecutor.AbortPolicy());
		}
	}
	
	static class DefaultThreadFactory implements ThreadFactory {
	    static final AtomicInteger poolNumber = new AtomicInteger(1);
	    final ThreadGroup group;
	    final AtomicInteger threadNumber = new AtomicInteger(1);
	    final String namePrefix;
	
	    DefaultThreadFactory() {
	        SecurityManager s = System.getSecurityManager();
	        group = (s != null)? s.getThreadGroup() :
	                             Thread.currentThread().getThreadGroup();
	        namePrefix = "TaskExePool-" + 
	                      poolNumber.getAndIncrement() + 
	                     "-TaskProcessor";
	    }
	
	    public Thread newThread(Runnable r) {
	        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
	        if (t.isDaemon())
	            t.setDaemon(false);
	        if (t.getPriority() != Thread.NORM_PRIORITY)
	            t.setPriority(Thread.NORM_PRIORITY);
	        return t;
	    }
	}

}