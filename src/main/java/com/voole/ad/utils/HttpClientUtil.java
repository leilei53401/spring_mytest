package com.voole.ad.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;



public class HttpClientUtil {
	protected static Logger logger = Logger.getLogger(HttpClientUtil.class);

	public static String httpGet(String url) throws ClientProtocolException, IOException {
		return httpGet(url,30000,30000);
	}
	
	public static String httpGet(String url,int socketTimeout,int connectTimeout) throws ClientProtocolException, IOException {
		String result="";
		CloseableHttpClient  client  =HttpClientBuilder.create().build();
		try {
			HttpGet httpGet = new HttpGet(url.trim());
			httpGet.setHeader("Content-Type","application/x-www-form-urlencoded;charset=UTF-8");
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout).build();//设置请求和传输超时时间  
			httpGet.setConfig(requestConfig);
			HttpResponse response  = client.execute(httpGet);
			int code = response.getStatusLine().getStatusCode();
			logger.debug("response code："+ code);
			HttpEntity entity = response.getEntity(); 
			result = EntityUtils.toString(entity,"UTF-8");
			client.close();
		} catch (Exception e){
			e.printStackTrace();
			logger.error("GET 请求["+url+"]出现异常：",e);
		}finally{
			if(client!=null){
				client.close();
			}
			logger.info("结束请求["+url+"]");
		}
		return result;
	}
	
	
	
	public static String httpPost(String url,List<NameValuePair> param) throws ClientProtocolException, IOException{
		return httpPost(url,param,30000,30000);
	}
	
	public static String httpPost(String url,List<NameValuePair> param,int socketTimeout,int connectTimeout) throws ClientProtocolException, IOException{
		String result="";
		CloseableHttpClient  client  =HttpClientBuilder.create().build();
		try{
			HttpPost httpost = new HttpPost(url.trim());
			httpost.setHeader("Content-Type","application/x-www-form-urlencoded;charset=UTF-8");
			HttpEntity entity = null;
			if(param==null){
				entity = new StringEntity("");
			}else{
				entity = new UrlEncodedFormEntity(param,"UTF-8");
			}
			httpost.setEntity(entity);
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout).build();//设置请求和传输超时时间  
			httpost.setConfig(requestConfig);
			HttpResponse response  = client.execute(httpost);
			HttpEntity responseEntity = response.getEntity(); 
			result = EntityUtils.toString(responseEntity,"utf-8");
			client.close();
		} finally{
			if(client!=null){
				client.close();
			}
		}
		return result;
	}

	
	public static String httpXmlPost(String url,String xmlString,int socketTimeout,int connectTimeout) throws ClientProtocolException, IOException{
		System.out.println("开始请求"+url);
		String result="";
		CloseableHttpClient  client  =HttpClientBuilder.create().build();
		try{
			HttpPost httpost = new HttpPost(url.trim());
			httpost.setHeader("Content-Type","text/xml;charset=UTF-8");
			HttpEntity  entity  = new StringEntity(xmlString);
			httpost.setEntity(entity);
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout).build();//设置请求和传输超时时间  
			httpost.setConfig(requestConfig);
			HttpResponse response  = client.execute(httpost);
			HttpEntity responseEntity = response.getEntity(); 
			result = EntityUtils.toString(responseEntity,"utf-8");
			client.close();
		} finally{
			if(client!=null){
				client.close();
			}
			System.out.println("结束请求"+url);
		}
		return result;
	}
	
	
	
	public static String httpImagePost(String url,String ImgPath,int socketTimeout,int connectTimeout) throws ClientProtocolException, IOException{
		String result="";
		CloseableHttpClient  client  =HttpClientBuilder.create().build();
		try{
			HttpPost httpost = new HttpPost(url);
			httpost.setHeader("Content-Type","image/jpeg;charset=UTF-8");
			HttpEntity  entity  = new InputStreamEntity(new FileInputStream(new File(ImgPath)));
			httpost.setEntity(entity);
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout).build();//设置请求和传输超时时间  
			httpost.setConfig(requestConfig);
			HttpResponse response  = client.execute(httpost);
			HttpEntity responseEntity = response.getEntity(); 
			result = EntityUtils.toString(responseEntity,"utf-8");
			client.close();
		} finally{
			if(client!=null){
				client.close();
			}
		}
		return result;
	}
	
	public static boolean download(String url, String fileUrl,int socketTimeout, int connectTimeout){  
		boolean result=false;
		CloseableHttpClient  client  =HttpClientBuilder.create().build();
		BufferedOutputStream bos = null;
		BufferedInputStream bis = null;
		try {
			HttpGet httpGet = new HttpGet(url.trim());
			httpGet.setHeader("Content-Type","application/x-www-form-urlencoded;charset=UTF-8");
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout).build();//设置请求和传输超时时间  
			httpGet.setConfig(requestConfig);
			HttpResponse response  = client.execute(httpGet);
			StatusLine statusLine = response.getStatusLine();
			if(statusLine.getStatusCode() == 200){
				HttpEntity entity = response.getEntity();
//				File file = new File(fileUrl);
//				file.deleteOnExit();
				bos = new BufferedOutputStream(new FileOutputStream(new File(fileUrl)));
				bis = new BufferedInputStream(entity.getContent());
				byte b[] = new byte[1024];  
                int j = 0;  
                while ((j = bis.read(b)) != -1) {  
                	bos.write(b, 0, j);  
                }
                bos.flush();
    			bos.close();
    			bis.close();
    			client.close();
    			result =true; 
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(bos!=null){
					bos.close();
				}
				if(bis!=null){
					bis.close();
				}
				if(client!=null){
					client.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
    }  
	
	
	
	public static String httpJsonPost(String url,String jsonString) throws ClientProtocolException, IOException{
		return httpJsonPost(url,jsonString,30000,30000);
	}
	
	public static String httpJsonPost(String url,String jsonString,int socketTimeout,int connectTimeout) throws ClientProtocolException, IOException{
		String result="";
		CloseableHttpClient  client  =HttpClientBuilder.create().build();
		try{
			HttpPost httpost = new HttpPost(url.trim());
			httpost.setHeader("Content-Type","application/json;charset=UTF-8");
			HttpEntity  entity  = new StringEntity(jsonString,"utf-8");
			httpost.setEntity(entity);
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout).build();//设置请求和传输超时时间  
			httpost.setConfig(requestConfig);
			HttpResponse response  = client.execute(httpost);
			int code = response.getStatusLine().getStatusCode();
			logger.debug("response code："+ code);
			HttpEntity responseEntity = response.getEntity(); 
			result = EntityUtils.toString(responseEntity,"utf-8");
			client.close();
		} catch (Exception e){
			logger.error("请求["+url+"]出现异常：",e);
		}finally{
			if(client!=null){
				client.close();
			}
			logger.info("结束请求["+url+"]");
		}
		return result;
	}
	
	
	public static void main(String[] args) throws ClientProtocolException, IOException {
		List<NameValuePair> param = new ArrayList<NameValuePair>();
//		param.add((new BasicNameValuePair("areaid","101010100")));
//		param.add((new BasicNameValuePair("type","forecast3d")));
//		param.add((new BasicNameValuePair("date","201411252031")));
//		param.add((new BasicNameValuePair("appid","995e0d")));
//		param.add((new BasicNameValuePair("key","ifPw6V7XkuKAykTRIC+nJkARY5s=")));
//		param.add((new BasicNameValuePair("fid","0000e997032000f4e28b41e55e0cb840")));
//		param.add((new BasicNameValuePair("status","3")));
//		download("http://open.weather.com.cn/data/?areaid=101010100&type=forecast3d&date=201411252031&appid=995e0d&key=ifPw6V7XkuKAykTRIC+nJkARY5s=","e:xxx.txt",10000,10000);
//		String post=httpPost("http://127.0.0.1:8080/operation_rmc/service/tsinfo/status",param);
// 		String get=httpGet("http://open.weather.com.cn/data/?areaid=101010100&type=forecast3d&date=201411252031&appid=995e0d&key=ifPw6V7XkuKAykTRIC+nJkARY5s=");
 		
 		String get=httpGet("http://rt.super-ssp.tv:8081/v1/a/1.gif?a=900109&b=123456789a&c=900109101&d=1010145432&e=15101010&f=b83d4e879e35&g=&h=1&i=36.110.66.39&j=23&k=2300&l=&m=&n=0&o=&p=&logo=123456789");
//		System.out.println(post);
		System.out.println(get);
	}
}
