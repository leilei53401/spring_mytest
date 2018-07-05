package com.voole.ad.preplan.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.voole.ad.loadtohdfs.HdfsOp;

public class StoreMacRredis {

	Logger logger = Logger.getLogger(StoreMacRredis.class);

	private String srcPath;

	private String hadoopUser;

	private String hdfsRootPath;

	private String dstPath;

	private CombMacServiceImp combMacServiceImp;
	
	//排期挑选,写入到文件
	public void storeSelectMacToFile(Map<String, List<String>> prePlanMap, String daytime, String planid)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmss");
		String starttime = sdf1.format(sdf.parse(daytime));
		String file = srcPath + File.separator + daytime + File.separator + planid + ".txt";
		File files = new File(file);
		RandomAccessFile randomFile = null;
		if (files.exists()) {
			try {
				randomFile = new RandomAccessFile(files, "rw");
				long fileLength = randomFile.length();
				randomFile.seek(fileLength);
				for (String key : prePlanMap.keySet()) {
					List<String> macList = prePlanMap.get(key);
					for (int i = 0; i < macList.size(); i++) {
						String mac = macList.get(i);
						String store = starttime + "," + mac + "," + "1" + "," + daytime + "," + key;
						randomFile.writeBytes(store);
						randomFile.writeBytes("\r\n");
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (null != randomFile) {
					try {
						randomFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			File files1 = new File(files.getParent());
			files1.mkdirs();
			if (files1.isDirectory()) {
				logger.info("------------------------文件夹:" + files.getParent() + "创建成功--------------------");
				try {
					files.createNewFile();
					randomFile = new RandomAccessFile(files, "rw");
					long fileLength = randomFile.length();
					randomFile.seek(fileLength);
					for (String key : prePlanMap.keySet()) {
						List<String> macList = prePlanMap.get(key);
						for (int i = 0; i < macList.size(); i++) {
							String mac = macList.get(i);
							String store = starttime + "," + mac + "," + "1" + "," + daytime + "," + key;
							randomFile.writeBytes(store);
							randomFile.writeBytes("\r\n");
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (null != randomFile) {
						try {
							randomFile.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	//新增终端存储到文件
	public void storeAddMacToFile(Map<String, List<String>> prePlanMap, String daytime, String planid)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmss");
		String starttime = sdf1.format(sdf.parse(daytime));
		String file = srcPath + File.separator + daytime + File.separator + planid + ".txt";
		String addFile = srcPath + File.separator + "add" + File.separator + daytime + File.separator + planid + ".txt";
		File addFiles = new File(addFile);
		File srcFile = new File(file);
		RandomAccessFile randomAddFile = null;
		RandomAccessFile randomFile = null;
		//Add新增文件
		if (addFiles.exists()) {
			try {
				randomAddFile = new RandomAccessFile(addFiles, "rw");
				long fileLength = randomAddFile.length();
				randomAddFile.seek(fileLength);
				for (String key : prePlanMap.keySet()) {
					List<String> macList = prePlanMap.get(key);
					for (int i = 0; i < macList.size(); i++) {
						String mac = macList.get(i);
						String store = starttime + "," + mac + "," + "1" + "," + daytime + "," + key;
						randomAddFile.writeBytes(store);
						randomAddFile.writeBytes("\r\n");
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (null != randomAddFile) {
					try {
						randomAddFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			File files1 = new File(addFiles.getParent());
			files1.mkdirs();
			if (files1.isDirectory()) {
				logger.info("------------------------文件夹:" + addFiles.getParent() + "创建成功--------------------");
				try {
					addFiles.createNewFile();
					randomAddFile = new RandomAccessFile(addFiles, "rw");
					long fileLength = randomAddFile.length();
					randomAddFile.seek(fileLength);
					for (String key : prePlanMap.keySet()) {
						List<String> macList = prePlanMap.get(key);
						for (int i = 0; i < macList.size(); i++) {
							String mac = macList.get(i);
							String store = starttime + "," + mac + "," + "1" + "," + daytime + "," + key;
							randomAddFile.writeBytes(store);
							randomAddFile.writeBytes("\r\n");
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (null != randomAddFile) {
						try {
							randomAddFile.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		//原始文件追加
		if (srcFile.exists()) {
			try {
				randomFile = new RandomAccessFile(srcFile, "rw");
				long fileLength = randomFile.length();
				randomFile.seek(fileLength);
				for (String key : prePlanMap.keySet()) {
					List<String> macList = prePlanMap.get(key);
					for (int i = 0; i < macList.size(); i++) {
						String mac = macList.get(i);
						String store = starttime + "," + mac + "," + "1" + "," + daytime + "," + key;
						randomFile.writeBytes(store);
						randomFile.writeBytes("\r\n");
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (null != randomFile) {
					try {
						randomFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			File files1 = new File(srcFile.getParent());
			files1.mkdirs();
			if (files1.isDirectory()) {
				logger.info("------------------------文件夹:" + srcFile.getParent() + "创建成功--------------------");
				try {
					srcFile.createNewFile();
					randomFile = new RandomAccessFile(srcFile, "rw");
					long fileLength = randomFile.length();
					randomFile.seek(fileLength);
					for (String key : prePlanMap.keySet()) {
						List<String> macList = prePlanMap.get(key);
						for (int i = 0; i < macList.size(); i++) {
							String mac = macList.get(i);
							String store = starttime + "," + mac + "," + "1" + "," + daytime + "," + key;
							randomFile.writeBytes(store);
							randomFile.writeBytes("\r\n");
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (null != randomFile) {
						try {
							randomFile.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	//将删除的mac终端从存储的文件中删除
	public void storeDelMacToFile(Map<String, List<String>> delPrePlanMap, String daytime, String planid)
			throws ParseException {
		String srcFile = srcPath + File.separator + daytime + File.separator + planid + ".txt";
		String tmpFile = srcPath + File.separator + daytime + File.separator + "tmp.txt";
		String delFile = srcPath + File.separator + "Del" + File.separator + daytime + File.separator + planid + ".txt";
		File files = new File(srcFile);
		if (files.exists()) {
			InputStreamReader isR = null;
			BufferedReader br = null;
			BufferedWriter delBw = null;
			BufferedWriter tmpBw = null;
			try {
				isR = new InputStreamReader(new FileInputStream(srcFile));
				br = new BufferedReader(isR);
				delBw = new BufferedWriter(new FileWriter(delFile));
				tmpBw = new BufferedWriter(new FileWriter(tmpFile));
				String line = null;
				for (String key : delPrePlanMap.keySet()) {
					List<String> macList = delPrePlanMap.get(key);
					for (int i = 0; i < macList.size(); i++) {
						String mac = macList.get(i);
						while ((line = br.readLine()) != null) {
							if (line.indexOf(mac) != -1) {
								delBw.write(line);
								delBw.newLine();
								continue;
							} else {
								tmpBw.write(line);
								tmpBw.newLine();
							}
						}
					}
				}
				tmpBw.close();
				delBw.close();
				br.close();
				File scFile = new File(srcFile);
				scFile.delete();
				File dstFile = new File(tmpFile);
				dstFile.renameTo(files);
				dstFile.delete();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (br != null || delBw != null || tmpBw != null) {
					try {
						delBw.close();
						tmpBw.close();
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public void httpPostRedis(String planid, String operate, List<String> list) {
		String macJson = JSONObject.toJSONString(list);
		try {
			String result = combMacServiceImp.httpJsonPost(planid, operate, macJson);
			JSONObject jsonObject = (JSONObject) JSONObject.parse(result);
			if ("true".equals(jsonObject.get("success").toString())) {
				list.clear();
			} else {
				httpPostRedis(planid, operate, list);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void storeMacToRedis(String daytime, String planid, String operate) {
		String file = "";
		String operateFile = srcPath + File.separator + daytime + File.separator + planid + ".txt";
		FileReader fr = null;
		BufferedReader br = null;
		if (operate.equals("store") || operate.equals("add")) {
			operate = "add";
			file = srcPath + File.separator + operate + File.separator + daytime + File.separator + planid + ".txt";
		} else if (operate.equals("del")) {
			file = srcPath + File.separator + operate + File.separator + daytime + File.separator + planid + ".txt";
		}
		try {
			File files = new File(file);
			if (files.exists()) {
				fr = new FileReader(files);
				br = new BufferedReader(fr);
				String line = "";
				String[] str;
				int count = 0;
				int num = 0;
				List<String> macList = new ArrayList<String>();
				while ((line = br.readLine()) != null) {
					str = line.split(",");
					String mac = str[1];
					macList.add(mac);
					count++;
					if ((count % 5000) == 0) {
						httpPostRedis(planid, operate, macList);
						num++;
						logger.info("----------------------------排期:" + daytime + "/" + planid + "批量" + operate
								+ "redis第" + num + "批Mac成功----------------------------");
						count = count - 5000;
					}
				}
				if (count < 5000) {
					httpPostRedis(planid, operate, macList);
					logger.info("----------------------------排期:" + daytime + "/" + planid + "-----" + operate
							+ "redis-Mac成功----------------------------");
				}
				if (operate.equals("add")) {
					files.delete();
				} else if (operate.equals("del")) {
					files.delete();
				}
				String fileName = planid + ".txt";
				// 上传文件前，先清空tmp_preplan_mac_status 表内容
				combMacServiceImp.truncateTable();
				logger.info("--------------------------已清空表:startpre.tmp_preplan_mac_status内的数据---------------");
				String table = "startpre.tmp_preplan_mac_status";
				combMacServiceImp.addTablePartition(table, daytime, planid);
				logger.info("--------------------------给表:startpre.tmp_preplan_mac_status添加" + daytime + File.separator
						+ planid + "分区 ---------------");
				String dstfPath = dstPath + "/" + "daytime=" + daytime + "/" + "planid=" + planid;
				boolean ifTrue = HdfsOp.uploadFile(false, true, operateFile, dstfPath, fileName, hadoopUser,
						hdfsRootPath);
				if (ifTrue) {
					logger.info("--------------------------上传文件" + fileName + "成功--------------------------");
					boolean ifcombMac = combMacServiceImp.combMacTable(daytime, planid);
					if (ifcombMac) {
						logger.info("--------------------------" + operate
								+ "mac------表tmp_preplan_mac_status 和 表 preplan_mac_status----数据合并成功--------------------------");
						boolean ifcombMacto = combMacServiceImp.combMacToPrePlan(daytime, planid);
						if (ifcombMacto) {
							logger.info("--------------------------排期:" + operate
									+ "mac----从表comb_preplan_mac_status 向 表 preplan_mac_status----导入数据成功--------------------------");
							logger.info("--------------------------排期:" + planid + "---" + operate
									+ "mac完成----------------");
						} else {
							logger.info("--------------------------排期:" + planid + "---" + operate
									+ "mac----从表comb_preplan_mac_status 向 表 preplan_mac_status----导入数据失败--------------------------");
						}
					} else {
						logger.info("--------------------------排期:" + planid + "---" + operate
								+ "mac--表tmp_preplan_mac_status 和 表 preplan_mac_status----无数据可以合并--------------------------");
					}
				} else {
					logger.info("--------------------------排期:" + planid + "---" + operate + "mac,上传文件" + fileName
							+ "失败--------------------------");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fr != null || br != null) {
				try {
					br.close();
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public String getSrcPath() {
		return srcPath;
	}

	public void setSrcPath(String srcPath) {
		this.srcPath = srcPath;
	}

	public String getHadoopUser() {
		return hadoopUser;
	}

	public void setHadoopUser(String hadoopUser) {
		this.hadoopUser = hadoopUser;
	}

	public String getHdfsRootPath() {
		return hdfsRootPath;
	}

	public void setHdfsRootPath(String hdfsRootPath) {
		this.hdfsRootPath = hdfsRootPath;
	}

	public String getDstPath() {
		return dstPath;
	}

	public void setDstPath(String dstPath) {
		this.dstPath = dstPath;
	}

	public CombMacServiceImp getCombMacServiceImp() {
		return combMacServiceImp;
	}

	public void setCombMacServiceImp(CombMacServiceImp combMacServiceImp) {
		this.combMacServiceImp = combMacServiceImp;
	}

}
