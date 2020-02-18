package edu.upc.essi.mongo.manager;

import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import org.bson.Document;

import java.sql.*;

public class E3_PostgreSQLManager {

	private static E3_PostgreSQLManager instance = null;
	private static CSVWriter writer;
	private String table;
	private float probability;
	private Connection JDBC;

	public static E3_PostgreSQLManager getInstance(String table, float probability, CSVWriter writer) throws Exception {
		if (instance == null)
			instance = new E3_PostgreSQLManager(table, probability, writer);
		return instance;
	}

	public E3_PostgreSQLManager(String table, float probability, CSVWriter writer2) throws Exception {
		this.table = table;
		this.writer = writer2;
		this.probability = probability;
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

		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_NULLS_ARE_TEXT (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_NULLS_ARE_NOTHING (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_NULLS_ARE_ZERO (ID CHAR(24), JSON JSONB)");

		JDBC.createStatement().execute("CREATE TABLE " + table + "_TUPLE_NULLS_ARE_TEXT (ID CHAR(24), a INT, b VARCHAR(10))");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_TUPLE_NULLS_ARE_ZERO (ID CHAR(24), a INT, b VARCHAR(10))");

		JDBC.commit();
	}

	public void insert(boolean isJSON, String kind) throws SQLException {
		Statement statement = JDBC.createStatement();
		if (isJSON) {
			E3_DocumentSet.getInstance().getByName(kind).stream().map(d -> {
				Document copy = Document.parse(d.toJson());
				String k = copy.remove("_id").toString();
				return "INSERT INTO " + table + "_JSON" + kind + "(ID,JSON) VALUES ('" + k + "','" + copy.toJson() + "')";
			}).forEach(s -> {
				try {
					statement.addBatch(s);
				} catch (SQLException exc) {
					exc.printStackTrace();
				}
			});
		} else {
			E3_DocumentSet.getInstance().getByName(kind).stream().map(d -> {
				Document copy = Document.parse(d.toJson());
				String k = copy.remove("_id").toString();
				Object a = copy.get("a");
				String b = copy.getString("b");
				return "INSERT INTO " + table + "_TUPLE" + kind + "(ID,a,b) VALUES ('" + k + "'," + a + ", '"+b+"')";
			}).forEach(s -> {
				try {
					statement.addBatch(s);
				} catch (SQLException exc) {
					exc.printStackTrace();
				}
			});
		}
		long startTime = System.nanoTime();
		statement.executeBatch();
		JDBC.commit();
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "insert",
				kind.substring(kind.lastIndexOf("_")+1), String.valueOf(1d-Math.pow(2,-probability)),
				String.valueOf(elapsedTime)});
	}

	public void sum(boolean isJSON, String kind) throws Exception {
		String sql = "";
		if (isJSON) {
			String expr = "sum((json->>'a')::int)";
			sql = "select "+expr+" from " + table + "_JSON" + kind;
		} else {
			sql = "select sum(a) from " + table + "_TUPLE" + kind;
		}
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "sum",
				kind.substring(kind.lastIndexOf("_")+1), String.valueOf(1d-Math.pow(2,-probability)),
				String.valueOf(elapsedTime)});
	}

	public void countNulls(boolean isJSON, String kind) throws Exception {
		String sql = "";
		if (isJSON) {
			String expr;
			if (kind.equals("_NULLS_ARE_TEXT") || kind.equals("_NULLS_ARE_NOTHING")) expr = "json->>'a' is null";
			else expr = "(json->>'a')::int = 0";
			sql = "select count(*) from " + table + "_JSON" + kind + " where "+expr;
		} else {
			if (kind.equals("_NULLS_ARE_TEXT"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a is null";
			else if (kind.equals("_NULLS_ARE_ZERO"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a=0";
		}
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "countNulls",
				kind.substring(kind.lastIndexOf("_")+1), String.valueOf(1d-Math.pow(2,-probability)),
				String.valueOf(elapsedTime)});

	}

	public void countNotNulls(boolean isJSON, String kind) throws Exception {
		String sql = "";
		if (isJSON) {
			String expr;
			if (kind.equals("_NULLS_ARE_TEXT") || kind.equals("_NULLS_ARE_NOTHING")) expr = "json->>'a' is not null";
			else expr = "(json->>'a')::int <> 0";
			sql = "select count(*) from " + table + "_JSON" + kind + " where "+expr;
		} else {
			if (kind.equals("_NULLS_ARE_TEXT"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a is not null";
			else if (kind.equals("_NULLS_ARE_ZERO"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a<>0";
		}
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "countNotNulls",
				kind.substring(kind.lastIndexOf("_")+1), String.valueOf(1d-Math.pow(2,-probability)),
				String.valueOf(elapsedTime)});
	}

	public void size(boolean isJSON, String kind) throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "size",
				String.valueOf(1d-Math.pow(2,-probability)),"", rs.getString(1) });
	}

	public void destroyme() {
		instance = null;
	}

}
