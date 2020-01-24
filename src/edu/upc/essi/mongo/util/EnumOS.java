package edu.upc.essi.mongo.util;

public enum EnumOS {
	LINUX("LINUX"),
	WINDOWS("WIN"),
	NOT_DEFINED("Not Defined");
	private String cod;
	
	EnumOS(String cod){
		this.cod = cod;
	}
	
	public String getCod() {
		return this.cod;
	}
}
