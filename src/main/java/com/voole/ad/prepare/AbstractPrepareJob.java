package com.voole.ad.prepare;

import org.apache.log4j.Logger;

import com.voole.ad.main.IJobLife;


/**
 * 数据准备类抽象类
 * @author shaoyl
 *
 */
public abstract class AbstractPrepareJob implements IJobLife {
	protected static Logger logger = Logger.getLogger(AbstractPrepareJob.class);

	@Override
	public void process(String time) {
		// TODO Auto-generated method stub
	}

}
