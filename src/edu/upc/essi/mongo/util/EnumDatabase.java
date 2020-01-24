package edu.upc.essi.mongo.util;

public enum EnumDatabase {
	POSTGRES("PO"),
	MONGO("MO"),
	POSTGRESXL("PX"),
	COACHDB("CO");
	private String cod;
	
	EnumDatabase(String cod){
		this.cod=cod;
	}
	public String getCod() {
		return cod;
	}
	public static EnumDatabase valueOfCod(String cod) {
	    for (EnumDatabase e : values()) {
	        if (e.cod.equals(cod)) {
	            return e;
	        }
	    }
	    return null;
	}
} 