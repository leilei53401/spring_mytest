package com.voole.ad.preplan.common;

import java.io.IOException;
import java.util.List;

public interface CombMacService {
	public List<String> selectMacPartition();
	
	public boolean combMacTable(String daytime, String planid);
	
	public List<String> selectMacPrePlanPartition();
	
	public boolean combMacToPrePlan(String daytime, String planid);
	
	public String httpJsonPost(String planid,String operate,String jsonString) throws IOException;
	
	public void truncateTable();
	
	public void addTablePartition(String table,String daytime, String planid);
	
}
