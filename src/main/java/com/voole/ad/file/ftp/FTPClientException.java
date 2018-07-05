package com.voole.ad.file.ftp;

import org.apache.log4j.Logger;

public class FTPClientException extends Exception {
	private static Logger logger = Logger.getLogger(FTPClientException.class);
	public FTPClientException(String msg) {
		super(msg);
		logger.error(msg);
	}

	public FTPClientException(String msg, Exception e) {
		super(msg,e);
		logger.error(msg,e);
	}
}
