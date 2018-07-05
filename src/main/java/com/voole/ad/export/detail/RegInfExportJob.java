package com.voole.ad.export.detail;

import com.voole.ad.export.AbastractBaseExportJob;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 注册接口报表，投放接口报表 单独处理
 * 增加过滤 无效 oemid
 * @author shaoyl
 *
 */
public  class RegInfExportJob extends AbastractBaseExportJob {


    private String oemInfoSql;

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}


    @Override
    public void process(String time) {

            logger.info("---------开始执行注册投放报表导出到Mysql任务-----------");
            long start = System.currentTimeMillis();
            int count = 0;

        // ########## 处理  oemid ##################

        List<Map<String, Object>> oemInfoList = adSupersspJt.queryForList(oemInfoSql);

        List<String> oemIdList = new ArrayList<String>();

        if (null != oemInfoList && oemInfoList.size() > 0) {

            for(int i=0;i<oemInfoList.size();i++){
                Map<String, Object> oemInfoMap = oemInfoList.get(i);
                String oemid = oemInfoMap.get("oemid").toString();
                oemIdList.add(oemid);
            }

        }else{
            logger.warn("未获取到OemInfo信息!");
        }

        String oemidsString = StringUtils.join(",",oemIdList);

        logger.info("获取到有效oemid为：["+oemidsString+"]");

// ########## 执行导出任务 ##################
            //执行hive
            for (Iterator iterator = exportTaskMap.keySet().iterator(); iterator.hasNext();) {

                String key = (String) iterator.next();  //配置Oracle表名
                String value  = exportTaskMap.get(key);//查询hive数据sql

                Long startTime = System.currentTimeMillis();

                value = value.replaceAll("@yestoday", time);

                value = value.replaceAll("@oemids", oemidsString);

                logger.info("开始执行导入表【"+key+"】日期为【"+time+"】的数据......");

                boolean result =  doSelect(value,key,time);

                logger.info("hivejob : 【"+time+"%"+key+"】 执行结果为："+result );
                count++;

                Long endTime = System.currentTimeMillis();

                logger.info("导入表【"+key+"】日期为【"+time+"】的数据完成，耗时 : ["+(endTime-startTime)/1000 +"] 秒!" );
            }

            long end = System.currentTimeMillis();

        logger.info("-------------------注册投放报表任务导出到Mysql任务【"+count+"】条执行完成，总耗时【"+(end-start)+"】毫秒！-------------------");

    }


    public String getOemInfoSql() {
        return oemInfoSql;
    }

    public void setOemInfoSql(String oemInfoSql) {
        this.oemInfoSql = oemInfoSql;
    }
}
