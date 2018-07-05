package com.voole.ad.file;


import java.io.File;
import java.io.FilenameFilter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.voole.ad.main.IJobLife;
import com.voole.ad.send.SendAliyunService;

/**
 * 解析本地文件
 * @author shaoyl
 *
 */
public abstract class AbastractBaseFileJob implements IJobLife {
	protected static Logger logger = Logger.getLogger(AbastractBaseFileJob.class);

	/**
	 * 执行job 中 hive jdbc的内容
	 * @param date
	 */
	@Override
	public void process(String time){}
	
	
}
