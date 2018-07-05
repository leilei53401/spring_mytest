package com.voole.ad.model.inventory;

import java.util.List;

public class Inventory {
	private String stamp;
	private List<Location> locations;

	public String getStamp() {
		return stamp;
	}
	public void setStamp(String stamp) {
		this.stamp = stamp;
	}
	public List<Location> getLocations() {
		return locations;
	}
	public void setLocations(List<Location> locations) {
		this.locations = locations;
	}
	
}
