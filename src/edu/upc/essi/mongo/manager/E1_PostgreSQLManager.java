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

import com.opencsv.CSVWriter;

import javax.json.Json;
import javax.json.JsonObject;

public class E1_PostgreSQLManager {

	private static E1_PostgreSQLManager instance = null;
	private static CSVWriter writer;
	private String table;
	private String schema;
	private Connection JDBC;
	private int size;

	public static E1_PostgreSQLManager getInstance(String table, String schema, CSVWriter writer) throws Exception {
		if (instance == null)
			instance = new E1_PostgreSQLManager(table, schema, writer);
		return instance;
	}

	public void resetInstance() {
		instance = null;
	}

	public E1_PostgreSQLManager(String table, String schema, CSVWriter writer2) throws Exception {
		this.table = table;
		this.schema = schema;
		this.writer = writer2;
		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
		this.size = obj.getJsonObject("properties").getJsonObject("A00").getInt("maxSize");

		Class.forName("org.postgresql.Driver");
		// Drop and create DB

		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres").createStatement().execute(""
				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");

//		DriverManager.getConnection("jdbc:postgresql://10.55.0.32/", "postgres", "user").createStatement().execute(""
//				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
//				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
//		JDBC = DriverManager.getConnection("jdbc:postgresql://10.55.0.32/ideas_experiments", "postgres", "user");

		JDBC.setAutoCommit(false);

		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withArray (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withAttributes (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute(
				"CREATE TABLE " + table + "_TUPLE_withAttributes (ID CHAR(24)," + getAttributesForE1(true) + ")");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_TUPLE_withArray (ID CHAR(24), \"A00\" int[])");
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
		long startTime = System.nanoTime();
		statement.executeBatch();
		JDBC.commit();
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", String.valueOf(size), "insert", "JSONWithArray",
				String.valueOf(elapsedTime) });
	}

	public void sumJSONWithArray() throws SQLException {
		String sql = "select sum(X.loc::int) from "
				+ "(SELECT (jsonb_array_elements_text(\"json\"->'A00')) as loc  FROM " + table
				+ "_JSON_withArray) as X";

		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(
				new String[] { "Postgres", String.valueOf(size), "sum", "JSONWithArray", String.valueOf(elapsedTime) });
	}

	public void sizeJSONWithArray() throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "_JSON_withArray') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(
				new String[] { "Postgres", String.valueOf(size), "size", "JSONWithArray", "", rs.getString(1) });
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
		long startTime = System.nanoTime();
		statement.executeBatch();
		JDBC.commit();
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", String.valueOf(size), "insert", "JSONWithAttributes",
				String.valueOf(elapsedTime) });
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

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", String.valueOf(size), "sum", "JSONWithAttributes",
				String.valueOf(elapsedTime) });

	}

	public void sizeJSONWithAttributes() throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "_JSON_withAttributes') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(
				new String[] { "Postgres", String.valueOf(size), "size", "JSONWithAttributes", "", rs.getString(1) });
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
		long startTime = System.nanoTime();
		statement.executeBatch();
		JDBC.commit();
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", String.valueOf(size), "insert", "TupleWithAttributes",
				String.valueOf(elapsedTime) });
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

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", String.valueOf(size), "sum", "TupleWithAttributes",
				String.valueOf(elapsedTime) });
	}

	public void sizeTupleWithAttributes() throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "_TUPLE_withAttributes') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(
				new String[] { "Postgres", String.valueOf(size), "size", "TupleWithAttributes", "", rs.getString(1) });
	}

	public void insertAsTupleWithArray() throws Exception {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			try {
				Document copy = Document.parse(d.toJson());
				String k = copy.remove("_id").toString();
				return "INSERT INTO " + table + "_TUPLE_withArray (ID,\"A00\") VALUES ('" + k + "','{"
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
		long startTime = System.nanoTime();
		statement.executeBatch();
		JDBC.commit();
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", String.valueOf(size), "insert", "TupleWithArray",
				String.valueOf(elapsedTime) });
	}

	public void sumTupleWithArray() throws SQLException {
		String sql = "SELECT  SUM((SELECT SUM(s) FROM UNNEST(\"A00\") s)) from  " + table + "_tuple_witharray;";

		PreparedStatement stmt = JDBC.prepareStatement(sql);

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", String.valueOf(size), "sum", "TupleWithArray",
				String.valueOf(elapsedTime) });
	}

	public void sizeTupleWithArray() throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "_tuple_witharray') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(
				new String[] { "Postgres", String.valueOf(size), "size", "TupleWithArray", "", rs.getString(1) });
	}

	public String getAttributesForE1(boolean withTypes) throws Exception {
		String out = "";
//		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
//		int size = obj.getJsonObject("properties").getJsonObject("theArray").getInt("maxSize");
		for (int i = 0; i < size; ++i) {

			if (i < 9)
				out += "A0" + (i + 1);
			else
				out += "A" + (i + 1);
			if (withTypes) {
				out += " int";
			}
			out += ",";
		}
		return out.substring(0, out.length() - 1);
	}

	public ArrayList<String> getAttributListForE1(boolean withTypes) throws Exception {
		ArrayList<String> list = new ArrayList<>();
//		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
//		int size = obj.getJsonObject("properties").getJsonObject("theArray").getInt("maxSize");
		for (int i = 0; i < size; ++i) {
			if (i < 9)
				list.add("A0" + (i + 1));
			else
				list.add("A" + (i + 1));
		}
		return list;
	}

	public String getValuesForE1(Document d) {
		return d.getList("A00", Integer.class).stream().map(i -> String.valueOf(i)).collect(Collectors.joining(","));
	}

	public String getAttributesAsDocumentForE1(Document d) {
		String out = "";
		for (int i = 0; i < d.getList("A00", Integer.class).size(); ++i) {
			if (i < 9)
				out += "\"A0" + (i + 1) + "\":" + d.getList("A00", Integer.class).get(i) + ",";
			else
				out += "\"A" + (i + 1) + "\":" + d.getList("A00", Integer.class).get(i) + ",";
		}
		return out.substring(0, out.length() - 1);
	}

}
