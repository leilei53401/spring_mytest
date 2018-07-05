package com.voole.ad.model.inventory;

public class Location {
	private String locatid;
	private Integer quantity;
	public String getLocatid() {
		return locatid;
	}
	public void setLocatid(String locatid) {
		this.locatid = locatid;
	}
	public Integer getQuantity() {
		return quantity;
	}
	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
	
	public boolean equals(Object obj){
		if (obj instanceof Location) {
			Location location = (Location) obj;
			return this.locatid.equals(location.getLocatid());
		}
		return super.equals(obj);
	}
}
