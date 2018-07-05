package com.voole.ad.aggre.detail;

import com.voole.ad.aggre.AbstractAggreTask;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashSet;

/**
 *  流量优化hive中计算匹配媒体方需要的mac
* @author shaoyl
* @date 2017-4-6 下午5:24:46 
* @version V1.0
 */
public class FlowOptAggreTask extends AbstractAggreTask {

    @Override
	public String doReplace() {
		return this.getTaskBody();
    }

}
