package edu.upc.essi.mongo.util;

public enum EnumOperation {
	SELECTALL("SEL"),
	INSERT("INS");
	private String operation;
	
	EnumOperation(String operation){
		this.operation = operation;
	}
	
	public String getOperation() {
		return operation;
	}
}
