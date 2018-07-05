package com.voole.ad.utils;

import com.alibaba.fastjson.JSON;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by shaoyl on 2018-6-1.
 */

public class SyncAliyunUtils {
    protected static Logger logger = Logger.getLogger(SyncAliyunUtils.class);
    //同步url
    private String urlPrefix;

    @Autowired
    public JdbcTemplate adSupersspJt;

    /**
     * 同步实时报表数据
     * @param table
     * @param syncTimeTag
     * @param syncTimeValue 注：时间戳特殊转化
     */
    public void syncRTDataToPlatform(String table,String syncTimeTag,String syncTimeValue,String replaceWords) {


        logger.info("------------start sync talbe 【"+table+"】,and "+syncTimeTag+"=["+syncTimeValue+"]-------------------------");
        long start  = System.currentTimeMillis();
        //查询数据

        String sql  = "select * from super_ssp_report."+table+" where "+syncTimeTag+"='"+syncTimeValue+"'";

        logger.info("get table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"] data sql is :"+sql);
        String rerportJson="";
        List<Map<String,Object>> reportList =  adSupersspJt.queryForList(sql);
        if(null!=reportList && reportList.size()>0){
            logger.info("获取到table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]数据【"+reportList.size()+"】条!");

            rerportJson = JSON.toJSONString(reportList);
            //转化时间戳，去掉特殊字符
            String syncTimeSendValue = syncTimeValue.replaceAll("-| |:","");

            String url = urlPrefix + table +"/"+syncTimeTag+"/"+syncTimeSendValue+"/"+replaceWords;

            logger.info("table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]上报URL:"+url);

            logger.debug("table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]上报json:"+rerportJson);

            String res = "";
            try {
                res = HttpClientUtil.httpJsonPost(url,rerportJson,60000,60000);
            } catch (ClientProtocolException e) {
                logger.error("table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]同步数据发生异常：",e);
            } catch (IOException e) {
                logger.error("table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]同步数据发生异常：",e);
            }
            if (StringUtils.isNotBlank(res)) {
                JSONObject jsonObject = JSONObject.fromObject(res);
                if ("0".equals(jsonObject.get("code").toString())) {
                    logger.info("table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]同步成功");
                } else {
                    logger.info("table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]同步失败："+jsonObject.get("message"));
                }
            }else{
                logger.warn("table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]同步数据返回为空！");
            }

        }else{
            logger.warn("未获取到table=["+table+"] "+syncTimeTag+"=["+syncTimeValue+"]的数据!");
        }
        long stop  = System.currentTimeMillis();
        logger.info("------------stop sync talbe ["+table+"],and "+syncTimeTag+"=["+syncTimeValue+"]--耗时["+(stop-start)+"]毫秒-----------------------");

    }



    public String getUrlPrefix() {
        return urlPrefix;
    }

    public void setUrlPrefix(String urlPrefix) {
        this.urlPrefix = urlPrefix;
    }
}
