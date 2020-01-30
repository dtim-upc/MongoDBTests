package edu.upc.essi.mongo.manager;

import java.io.StringReader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.stream.Collectors;

import edu.upc.essi.mongo.datagen.DocumentSet;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import javax.json.Json;
import javax.json.JsonObject;

public class E1_PostgreSQLManager {

	private static E1_PostgreSQLManager instance = null;
	private String table;
	private String schema;
	private Connection JDBC;

	public static E1_PostgreSQLManager getInstance(String table, String schema) throws Exception {
		if (instance == null)
			instance = new E1_PostgreSQLManager(table, schema);
		return instance;
	}

	public E1_PostgreSQLManager(String table, String schema) throws Exception {
		this.table = table;
		this.schema = schema;

		Class.forName("org.postgresql.Driver");
		// Drop and create DB
		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres").createStatement().execute(""
				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");

		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");
		JDBC.setAutoCommit(false);

		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withArray (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withAttributes (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute(
				"CREATE TABLE " + table + "_TUPLE_withAttributes (ID CHAR(24)," + getAttributesForE1(true) + ")");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_TUPLE_withArray (ID CHAR(24), theArray int[])");
		JDBC.commit();
	}

	public void insertAsJSONWithArray() throws SQLException {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			Document copy = Document.parse(d.toJson());
			String k = copy.remove("_id").toString();
			return "INSERT INTO " + table + "_JSON_withArray(ID,JSON) VALUES ('" + k + "','" + copy.toJson() + "')";
		}).forEach(s -> {
			try {
				statement.addBatch(s);
			} catch (SQLException exc) {
				exc.printStackTrace();
			}
		});
		statement.executeBatch();
		JDBC.commit();
	}

	public void sumJSONWithArray() throws SQLException {
		String sql = "SELECT sum(loc)  from (SELECT  (jsonb_array_elements(o.\"json\"->'val'))::numeric loc  FROM  "
				+ table + "_JSON_withArray o) a";

		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);

		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
	}

	public void insertAsJSONWithAttributes() throws SQLException {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			Document copy = Document.parse(d.toJson());
			String k = copy.remove("_id").toString();
			return "INSERT INTO " + table + "_JSON_withAttributes(ID,JSON) VALUES ('" + k + "','{"
					+ getAttributesAsDocumentForE1(copy) + "}')";
		}).forEach(s -> {
			try {
				statement.addBatch(s);
			} catch (SQLException exc) {
				exc.printStackTrace();
			}
		});
		statement.executeBatch();
		JDBC.commit();
	}

	public void sumJSONWithAttributes() throws Exception {

		ArrayList<String> attribs = getAttributListForE1(false);
		StringBuilder sb = new StringBuilder("SELECT  SUM(");

		for (String string : attribs) {
			sb.append("(\"json\"->>'").append(string).append("')::int+");
		}

		if (sb.length() > 0)
			sb.deleteCharAt(sb.length() - 1);

		sb.append(") FROM ").append(table).append("_JSON_withAttributes");
		System.out.println(sb.toString());

		PreparedStatement stmt = JDBC.prepareStatement(sb.toString());

		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));

	}

	public void insertAsTupleWithAttributes() throws Exception {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			Document copy = Document.parse(d.toJson());
			String k = copy.remove("_id").toString();
			try {
				return "INSERT INTO " + table + "_TUPLE_withAttributes (ID," + getAttributesForE1(false) + ") VALUES ('"
						+ k + "'," + getValuesForE1(copy) + ")";
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}).forEach(s -> {
			try {
				statement.addBatch(s);
			} catch (SQLException exc) {
				exc.printStackTrace();
			}
		});
		statement.executeBatch();
		JDBC.commit();
	}

	public void sumTupleWithAttributes() throws Exception {
		ArrayList<String> attribs = getAttributListForE1(false);
		StringBuilder sb = new StringBuilder("SELECT  SUM(");

		for (String string : attribs) {
			sb.append(string + "+");
		}
		// remove the trailing +
		if (sb.length() > 0)
			sb.deleteCharAt(sb.length() - 1);

		sb.append(") FROM ").append(table).append("_TUPLE_withAttributes");
		System.out.println(sb.toString());

		PreparedStatement stmt = JDBC.prepareStatement(sb.toString());

		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
	}

	public void insertAsTupleWithArray() throws Exception {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			try {
				Document copy = Document.parse(d.toJson());
				String k = copy.remove("_id").toString();
				return "INSERT INTO " + table + "_TUPLE_withArray (ID,theArray) VALUES ('" + k + "','{"
						+ getValuesForE1(copy) + "}')";
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}).forEach(s -> {
			try {
				statement.addBatch(s);
			} catch (SQLException exc) {
				exc.printStackTrace();
			}
		});
		statement.executeBatch();
		JDBC.commit();
	}

	public void sumTupleWithArray() throws SQLException {
		String sql = "SELECT  SUM((SELECT SUM(s) FROM UNNEST(\"thearray\") s)) from  " + table + "_tuple_witharray;";

		PreparedStatement stmt = JDBC.prepareStatement(sql);

		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
	}

	public String getAttributesForE1(boolean withTypes) throws Exception {
		String out = "";
		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
		int size = obj.getJsonObject("properties").getJsonObject("theArray").getInt("maxSize");
		for (int i = 0; i < size; ++i) {
			out += "a" + i;
			if (withTypes) {
				out += " int";
			}
			out += ",";
		}
		return out.substring(0, out.length() - 1);
	}

	public ArrayList<String> getAttributListForE1(boolean withTypes) throws Exception {
		ArrayList<String> list = new ArrayList<>();
		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
		int size = obj.getJsonObject("properties").getJsonObject("theArray").getInt("maxSize");
		for (int i = 0; i < size; ++i) {
			list.add("a" + i);
		}
		return list;
	}

	public String getValuesForE1(Document d) {
		return d.getList("theArray", Integer.class).stream().map(i -> String.valueOf(i))
				.collect(Collectors.joining(","));
	}

	public String getAttributesAsDocumentForE1(Document d) {
		String out = "";
		for (int i = 0; i < d.getList("theArray", Integer.class).size(); ++i) {
			out += "\"a" + i + "\":" + d.getList("theArray", Integer.class).get(i) + ",";
		}
		return out.substring(0, out.length() - 1);
	}

}
