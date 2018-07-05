package com.voole.ad.utils;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpConnectionManager {   
  
		private Logger logger = LoggerFactory.getLogger(HttpConnectionManager.class);
	    /** 
	     * 最大连接数 
	     */  
	    public final static int MAX_TOTAL_CONNECTIONS = 1000;  
	    /** 
	     * 获取连接的最大等待时间 
	     */  
	    public final static int WAIT_TIMEOUT = 6000;  
	    /** 
	     * 每个路由最大连接数 
	     */  
	    public final static int MAX_ROUTE_CONNECTIONS = 1000;  
	    /** 
	     * 连接超时时间 
	     */  
	    public final static int CONNECT_TIMEOUT = 10000;  
	    /** 
	     * 读取超时时间 
	     */  
	    public final static int READ_TIMEOUT = 10000;  
	   
	  
	    public static CloseableHttpClient  getHttpClient(String url, Integer port) {  
	        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
	        // 将最大连接数增加到200
	        cm.setMaxTotal(MAX_TOTAL_CONNECTIONS);
	        // 将每个路由基础的连接增加到20
	        cm.setDefaultMaxPerRoute(MAX_ROUTE_CONNECTIONS);
	        cm.setValidateAfterInactivity(8000);
	        //HttpHost localhost = new HttpHost(url, port);
	        //将目标主机的最大连接数增加到50
//	        cm.setMaxPerRoute(new HttpRoute(localhost), CONNECT_TIMEOUT);
	        RequestConfig globalConfig = RequestConfig.custom()
	        		.setCookieSpec(CookieSpecs.IGNORE_COOKIES)
	        		.build();
	        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(globalConfig).setConnectionManager(cm).build();
	        return httpClient;
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