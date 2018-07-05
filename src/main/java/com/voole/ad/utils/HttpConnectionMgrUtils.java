package com.voole.ad.utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpConnectionMgrUtils {   
  
		private Logger logger = LoggerFactory.getLogger(HttpConnectionMgrUtils.class);
		
	    public final static int MAX_TOTAL_CONNECTIONS = Integer.parseInt(GlobalProperties.getProperties("MAX_TOTAL_CONNECTIONS").trim());  //4000
	
	    public final static int MAX_ROUTE_CONNECTIONS = Integer.parseInt(GlobalProperties.getProperties("MAX_ROUTE_CONNECTIONS").trim());//1000;  

	    public final static int CONNECT_TIMEOUT = Integer.parseInt(GlobalProperties.getProperties("CONNECT_TIMEOUT").trim());//10000;  
	
	    public final static int CONNECTION_REQUEST_TIMEOUT = Integer.parseInt(GlobalProperties.getProperties("CONNECTION_REQUEST_TIMEOUT").trim());
	   
	    public final static int SOCKET_TIMEOUT =Integer.parseInt(GlobalProperties.getProperties("SOCKET_TIMEOUT").trim());
	    
	    private CloseableHttpAsyncClient asyncHttpClient = null;
	   
	    private PoolingNHttpClientConnectionManager connManager = null;
	 
	    private ConnectingIOReactor ioReactor = null;
	    public HttpConnectionMgrUtils(){
	    	initHttpClient();//初始化asynchttpclient
	    }
	  
	    public void initHttpClient() {  
	    	 // Create global request configuration
		    IOReactorConfig config = IOReactorConfig.custom()
		    						 .setTcpNoDelay(true)
		    						 .setSoReuseAddress(true)
		    						 .setConnectTimeout(CONNECT_TIMEOUT)
		    						 .setSoKeepAlive(false)
		    						 .setSoTimeout(SOCKET_TIMEOUT)
		    						 .build();
	    	
			try {
				ioReactor = new DefaultConnectingIOReactor(config);
			} catch (IOReactorException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	connManager = new PoolingNHttpClientConnectionManager(ioReactor);
		    connManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
		    connManager.setDefaultMaxPerRoute(MAX_ROUTE_CONNECTIONS);
	    	asyncHttpClient =  HttpAsyncClients.custom().disableCookieManagement()
	        		 .setConnectionManager(connManager)
	        		 .build();
	        asyncHttpClient.start();
	    }  
	    
	    
	    
	    /**
	     * @param adinfo
	     * @return
	     */
	    public int invokeGetUrl(String sendUrl){
    	     int statusCode = 0; 
//	    	 final String sendUrl = adinfo.getParams();
			 final HttpGet request = new HttpGet(sendUrl);
			 request.addHeader("Connection", "close");//短连接配置
			 final Future<HttpResponse> futureResponse;
			 final CountDownLatch latch = new CountDownLatch(1);
			 futureResponse = asyncHttpClient.execute(request, new FutureCallback<HttpResponse>() {
				@Override
				public void completed(HttpResponse result) {
					 latch.countDown();
				}
				@Override
				public void failed(Exception ex) {
					latch.countDown();
				}
				@Override
				public void cancelled() {
					latch.countDown();
				}
			});
			 
			try {
				latch.await();
				statusCode = futureResponse.get().getStatusLine().getStatusCode();
			} catch (InterruptedException | ExecutionException e) {
				int random = (int) (Math.random()*10000);
				if(random>=0 && random <=1000){
					logger.error("send error! "+statusCode+", "+sendUrl,e);
				}
			}finally{
				if(request != null){
					request.releaseConnection(); //释放关闭连接
				}
			}
			return statusCode;
	    }
	    
	    /**
	     * 关闭连接
	     * @param httpClient
	     */
	    public static void close(CloseableHttpClient httpClient){
	    	try {
				httpClient.close();
			} catch (IOException e) {
				
			}
	    }
} 