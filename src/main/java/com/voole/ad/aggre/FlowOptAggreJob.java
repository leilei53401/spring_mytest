package com.voole.ad.aggre;

import com.voole.ad.main.IJobLife;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;


/**
 * 汇聚hive中的数据,执行hive汇聚sql
 * @author shaoyl
 *
 */
public class FlowOptAggreJob {
	protected static Logger logger = Logger.getLogger(FlowOptAggreJob.class);

	@Autowired
	public JdbcTemplate hiveJt;

//    private Map<String,HashSet<String>> mediaAndOemids;
    //计算任务列表
    protected Map<String, AbstractAggreTask> agreeTaskMap;
	/**
	 * 执行job 中 hive jdbc的内容
	 * @param date
	 */
	public void process(String date, Map<String,HashSet<String>> mediaAndOemids){
		
		logger.info("---------开始执行汇聚任务-----------");
		long start = System.currentTimeMillis();

        if(mediaAndOemids!=null && mediaAndOemids.size()>0) {
            logger.info("进入流量优化计算任务，获取到媒体【"+mediaAndOemids.size()+"】条！");
            //循环媒体计算
            for (Iterator iterator = mediaAndOemids.keySet().iterator(); iterator.hasNext(); ) {


                String mediaid = (String) iterator.next();

                HashSet<String> oemidSet = mediaAndOemids.get(mediaid);

                String oemids = "-1";

                if(null!=oemidSet && oemidSet.size()>0){
                    oemids =  getOemidsBySet(oemidSet);
                }

                logger.info("======== start process mediaid = ["+mediaid+"] task =============");

                for (Iterator iteratorTask = agreeTaskMap.keySet().iterator(); iteratorTask.hasNext();) {

                    String key = (String) iteratorTask.next();
                    AbstractAggreTask aggreTask  = agreeTaskMap.get(key);
                    logger.info(" hivejob ：【"+mediaid+"%"+date+"%"+key+"】flag=["+aggreTask.isEnable()+"]......");
                    if(aggreTask.isEnable()){

                        long startTime = System.currentTimeMillis();

                        String mediaAggreSql = aggreTask.doReplace();

                        mediaAggreSql = mediaAggreSql.replaceAll("@mediaid", mediaid);

                        mediaAggreSql = mediaAggreSql.replaceAll("@oemid", oemids);

                        mediaAggreSql = mediaAggreSql.replaceAll("@yestoday", date);

                        String jobName = "flowopt_" + mediaid + "_" + key + "_" + date;

                        logger.info("开始执行 hivejob ：【" + jobName + "】的汇聚任务......");

                        String setJobName = "set mapred.job.name=" + jobName;

                        logger.info("汇聚sql语句为：" + mediaAggreSql);

                        try {
                            hiveJt.execute(setJobName);
                            hiveJt.execute(mediaAggreSql);
                        } catch (Exception e) {
                            logger.error("执行执行hivejob 【" + jobName + "】的汇聚任务出错，出错sql为[" + mediaAggreSql + "],异常信息为:", e);
                        }

                        long endTime = System.currentTimeMillis();

                        logger.info("结束执行 hivejob ：【" + jobName + "】的汇聚任务,耗时["+(endTime-startTime)+"]毫秒！");

                    }


                }

                logger.info("======== end process mediaid = ["+mediaid+"] task =============");


            }

        }else{
            logger.info("没有流量优化汇聚任务需要执行！");
        }

		
		//执行hive
		/*for (Iterator iterator = agreeTaskMap.keySet().iterator(); iterator.hasNext();) {

			long startTime = System.currentTimeMillis();
	        String key = (String) iterator.next(); 
	        AbstractAggreTask aggreTask  = agreeTaskMap.get(key);
	        logger.info("hivejob ：【"+date+"%"+key+"】flag=["+aggreTask.isEnable()+"]......");
	        if(aggreTask.isEnable()){
		        aggreTask.setDate(date);
		        String sql = aggreTask.doReplace();
				logger.info("开始执行hivejob ：【"+date+"%"+key+"】的汇聚任务......");
	
				String jobName ="set mapred.job.name="+date+"%"+key;
								
				logger.info("汇聚sql语句为："+sql);
				
				try {
					hiveJt.execute(jobName);
					hiveJt.execute(sql);
				} catch (Exception e) {
					logger.error("执行执行hivejob 【"+date+"%"+key+"】的汇聚任务出错，出错sql为["+sql+"],异常信息为:",e);
				}

				count++;
				
				long endTime = System.currentTimeMillis();
				
				logger.info("hivejob : 【"+date+"%"+key+"】 执行 hive sql 操作 耗时 : ["+(endTime-startTime)/1000/60 +"]分!" );
	        }
	    }*/
		
		long end = System.currentTimeMillis();
		
		logger.info("-------------------流量优化汇聚任务执行完成，总耗时【"+(end-start)+"】毫秒！-------------------");
	
	}

	public Map<String, AbstractAggreTask> getAgreeTaskMap() {
		return agreeTaskMap;
	}

	public void setAgreeTaskMap(Map<String, AbstractAggreTask> agreeTaskMap) {
		this.agreeTaskMap = agreeTaskMap;
	}

//    public Map<String, HashSet<String>> getMediaAndOemids() {
//        return mediaAndOemids;
//    }
//
//    public void setMediaAndOemids(Map<String, HashSet<String>> mediaAndOemids) {
//        this.mediaAndOemids = mediaAndOemids;
//    }

    /**
     * 获取oemid列表
     * @param oemidSet
     * @return
     */
    private String getOemidsBySet(HashSet<String> oemidSet){
        return StringUtils.join(oemidSet,",");
    }
}
