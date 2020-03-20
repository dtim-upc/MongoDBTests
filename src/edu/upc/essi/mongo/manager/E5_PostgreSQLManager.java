package edu.upc.essi.mongo.manager;

import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.bson.Document;
import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

public class E5_PostgreSQLManager {

	private static E5_PostgreSQLManager instance = null;
	private static CSVWriter writer;
	private String table;
	private int attribs;
	private int nAttributes;
	private Connection JDBC;
	private boolean isCount;

	public static E5_PostgreSQLManager getInstance(String table, int nLevels, int nAttributes, CSVWriter writer,
			boolean iscnt) throws Exception {
		if (instance == null)
			instance = new E5_PostgreSQLManager(table, nLevels, nAttributes, writer, iscnt);
		return instance;
	}

	public E5_PostgreSQLManager(String table, int nLevels, int nAttributes, CSVWriter writer2, boolean iscnt)
			throws Exception {
		this.table = table;
		this.attribs = nLevels;
		this.nAttributes = nAttributes;
		this.writer = writer2;
		this.isCount = iscnt;

		Class.forName("org.postgresql.Driver");
		// Drop and create DB

		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres").createStatement().execute(""
		+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
		+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");

//		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "TYPsm3").createStatement().execute(""
//				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
//				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
//		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "TYPsm3");

//		DriverManager.getConnection("jdbc:postgresql://10.55.0.32/", "postgres", "user").createStatement().execute(""
//				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
//				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
//		JDBC = DriverManager.getConnection("jdbc:postgresql://10.55.0.32/ideas_experiments", "postgres", "user");

		JDBC.setAutoCommit(false);

		JDBC.createStatement().execute("CREATE TABLE " + table + "(ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "tuple (ID CHAR(24)," + getAttributes(true) + ")");
		JDBC.commit();
	}

	public void insert() throws SQLException {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			Document copy = Document.parse(d.toJson());
			String k = copy.remove("_id").toString();
			return "INSERT INTO " + table + "(ID,JSON) VALUES ('" + k + "','" + copy.toJson() + "')";
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
		writer.writeNext(new String[] { "Postgres", "insert", table.substring(table.lastIndexOf("_") + 1),
				String.valueOf(attribs), String.valueOf(nAttributes), String.valueOf(elapsedTime) });
	}

	public void insertTuple() throws Exception {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			Document copy = Document.parse(d.toJson());
			String k = copy.remove("_id").toString();
			return "INSERT INTO " + table + "tuple (ID," + getAttributes(false) + ") VALUES ('" + k + "',"
					+ getValuesForE(copy) + ")";
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
		writer.writeNext(new String[] { "Postgres", "insert", table.substring(table.lastIndexOf("_") + 1) + "tuple",
				String.valueOf(attribs), String.valueOf(nAttributes), String.valueOf(elapsedTime) });
	}

	private String getValuesForE(Document d) {
		// TODO Auto-generated method stub

		if (isCount) {
			return d.values().stream().map(i -> String.valueOf(i)).collect(Collectors.joining(","));
		} else {
			StringBuilder sb = new StringBuilder();
			String[] arr = getAttributes(false).split(",");
			for (int j = 0; j < 10; j++) {
				if (j == 9) {
					sb.append(d.get(arr[j]) + ",");
				} else {
					sb.append("'" + d.get(arr[j]) + "',");
				}
			}

			return sb.substring(0, sb.length() - 1).toString();
		}

	}

	public void sum(boolean isLength) throws Exception {
		String path = "sum((json->>'a";
		if (isLength) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < attribs; i++) {
				sb.append("0");
			}
			path += sb.toString();
		}

		path += (nAttributes < 10 ? '0' + String.valueOf(nAttributes) : nAttributes) + "'" + ")::int) ";

		String sql = "select " + path + " from " + table;
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", "sum", table.substring(table.lastIndexOf("_") + 1),
				String.valueOf(attribs), String.valueOf(nAttributes), String.valueOf(elapsedTime) });
	}

	public void sumtuple(boolean isLength) throws Exception {

		StringBuilder sb = new StringBuilder("SELECT  SUM(a");

		if (isLength) {
			for (int i = 0; i < attribs; i++) {
				sb.append("0");
			}
		}

		sb.append((nAttributes < 10 ? '0' + String.valueOf(nAttributes) : nAttributes));

		sb.append(") FROM ").append(table).append("tuple");
		System.out.println(sb.toString());

		PreparedStatement stmt = JDBC.prepareStatement(sb.toString());

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", "sum", table.substring(table.lastIndexOf("_") + 1) + "tuple",
				String.valueOf(attribs), String.valueOf(nAttributes), String.valueOf(elapsedTime) });
	}

	public void size() throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { "Postgres", "size", table.substring(table.lastIndexOf("_") + 1),
				String.valueOf(attribs), String.valueOf(nAttributes), "", rs.getString(1) });
	}

	public void sizetuple() throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "tuple') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { "Postgres", "size", table.substring(table.lastIndexOf("_") + 1) + "tuple",
				String.valueOf(attribs), String.valueOf(nAttributes), "", rs.getString(1) });
	}

	public void destroyme() {
		instance = null;
	}

	public String getAttributes(boolean withTypes) {
		String out = "";
//		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
//		int size = obj.getJsonObject("properties").getJsonObject("theArray").getInt("maxSize");
		int size = isCount ? nAttributes : 10;
		if (isCount) {
			for (int i = size; i > size - attribs; i--) {
				out += "a" + (i < 10 ? '0' + String.valueOf(i) : String.valueOf(i));
				if (withTypes) {
					out += " int";
				}
				out += ",";
			}
		} else {
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < attribs; i++) {
				sb.append("0");
			}

			int fixedattibs = 10;
			for (int i = 1; i < fixedattibs + 1; i++) {
				JsonObjectBuilder A = Json.createObjectBuilder();
				if (i == fixedattibs) {
					out += "a" + sb.toString() + (i < 10 ? '0' + String.valueOf(i) : String.valueOf(i));
					if (withTypes) {
						out += " int";
					}
				} else {
					out += "a" + sb.toString() + (i < 10 ? '0' + String.valueOf(i) : String.valueOf(i));
					if (withTypes) {
						out += " char(" + (128 - attribs + 1) + ")";
					}
				}

				out += ",";
			}
		}

		return out.substring(0, out.length() - 1);
	}

}
