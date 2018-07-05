package com.voole.ad.extopt;

import org.apache.log4j.Logger;

import com.voole.ad.main.IJobLife;


/**
 *  导入关系型数据库后扩展任务
 * @author shaoyl
 *
 */
public abstract class AbstractExtOptJob implements IJobLife {
	protected static Logger logger = Logger.getLogger(AbstractExtOptJob.class);

	@Override
	public void process(String time) {
		// TODO Auto-generated method stub
	}

}
