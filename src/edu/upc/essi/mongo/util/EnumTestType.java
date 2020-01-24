package edu.upc.essi.mongo.util;

public enum EnumTestType { 
		NOPK_NOCOD_RNF("01"), 
		PK_NOCOD_RNF("02"),
		NOPK_NOCOD_JSON("21"),
		PK_NOCOD_JSON("22"),
		NOPK_COD_JSON("31"),
		PK_COD_JSON("32"),
		NOPK_NOCOD_JSONB("41"),
		PK_NOCOD_JSONB("42"),
		NOPK_COD_JSONB("51"),
		PK_COD_JSONB("52"),
		NOT_IMPLEMENTED("--NotImplemented--");
		private String cod;
		
		EnumTestType(String cod){
			this.cod = cod;
		}
		
		public String getCod() {
			return cod;
		}
		
		public boolean hadPK() {
			switch (this) {
				case NOPK_COD_JSON:
				case NOPK_NOCOD_JSON:
				case NOPK_NOCOD_JSONB:
				case NOPK_COD_JSONB:
				case NOPK_NOCOD_RNF:
					return false;
				case PK_COD_JSON:
				case PK_COD_JSONB:
				case PK_NOCOD_JSON:
				case PK_NOCOD_JSONB:
				case PK_NOCOD_RNF:
					return true;
				default:
					return false;
			}
		}
		public boolean isEncoded() {
			switch (this) {
				case NOPK_COD_JSON:
				case NOPK_COD_JSONB:
				case PK_COD_JSON:
				case PK_COD_JSONB:
					return true;
				case NOPK_NOCOD_JSON:
				case NOPK_NOCOD_JSONB:
				case NOPK_NOCOD_RNF:
				case PK_NOCOD_JSON:
				case PK_NOCOD_JSONB:
				case PK_NOCOD_RNF:
					return false;
				default:
					return false;
			}
		}
		public String getFormat() {
			switch (this) {
				case NOPK_COD_JSON:
				case PK_COD_JSON:
				case NOPK_NOCOD_JSON:
				case PK_NOCOD_JSON:
					return "JSON";
				case NOPK_COD_JSONB:
				case PK_COD_JSONB:
				case NOPK_NOCOD_JSONB:
				case PK_NOCOD_JSONB:
					return "JSONB";
				case NOPK_NOCOD_RNF:
				case PK_NOCOD_RNF:
					return "RNF";
				default:
					return "";
			}
		}
		public static EnumTestType valueOfCod(String cod) {
		    for (EnumTestType e : values()) {
		        if (e.cod.equals(cod)) {
		            return e;
		        }
		    }
		    return null;
		}
}
