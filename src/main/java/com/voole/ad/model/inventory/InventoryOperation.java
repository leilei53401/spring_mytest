package com.voole.ad.model.inventory;

public class InventoryOperation {
	private String operation;
	private String operationId;
	private Inventory realtimeinventory;
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getOperationId() {
		return operationId;
	}
	public void setOperationId(String operationId) {
		this.operationId = operationId;
	}
	public Inventory getRealtimeinventory() {
		return realtimeinventory;
	}
	public void setRealtimeinventory(Inventory realtimeinventory) {
		this.realtimeinventory = realtimeinventory;
	}
}
