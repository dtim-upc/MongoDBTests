package edu.upc.essi.mongo.manager;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
	
	public void reconnect() {
		try {
			JDBC = DriverManager.getConnection("jdbc:postgresql://10.55.0.32/ideas_experiments", "postgres", "user");
			//JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");
			JDBC.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public E3_PostgreSQLManager(String table, float probability, CSVWriter writer2) throws Exception {
		this.table = table;
		this.writer = writer2;
		this.probability = probability;
		Class.forName("org.postgresql.Driver");
		// Drop and create DB

//		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres").createStatement().execute(""
//		+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
//		+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
//		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");

		DriverManager.getConnection("jdbc:postgresql://10.55.0.32/", "postgres", "user").createStatement().execute(""
				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
		JDBC = DriverManager.getConnection("jdbc:postgresql://10.55.0.32/ideas_experiments", "postgres", "user");

		JDBC.setAutoCommit(false);

		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_NULLS_ARE_TEXT (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_NULLS_ARE_NOTHING (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_NULLS_ARE_ZERO (ID CHAR(24), JSON JSONB)");

		JDBC.createStatement()
				.execute("CREATE TABLE " + table + "_TUPLE_NULLS_ARE_TEXT (ID CHAR(24), "
						+ IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)) + " double precision")
								.sorted().collect(Collectors.joining(","))
						+", b VARCHAR(64))");
		JDBC.createStatement()
				.execute("CREATE TABLE " + table + "_TUPLE_NULLS_ARE_ZERO (ID CHAR(24), "
						+ IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)) + " double precision")
						.sorted().collect(Collectors.joining(","))
						+", b VARCHAR(64))");

		JDBC.commit();
	}
	
	public void analyze() throws SQLException {
		JDBC.createStatement().execute("ANALYZE " + table + "_JSON_NULLS_ARE_TEXT");
		JDBC.createStatement().execute("ANALYZE " + table + "_JSON_NULLS_ARE_NOTHING");
		JDBC.createStatement().execute("ANALYZE " + table + "_JSON_NULLS_ARE_ZERO");

		JDBC.createStatement()
				.execute("ANALYZE " + table + "_TUPLE_NULLS_ARE_TEXT ");
		JDBC.createStatement()
				.execute("ANALYZE " + table + "_TUPLE_NULLS_ARE_ZERO ");

		JDBC.commit();
	}

	public void insert(boolean isJSON, String kind) throws SQLException {
		Statement statement = JDBC.createStatement();
		if (isJSON) {
			E3_DocumentSet.getInstance().getByName(kind).stream().map(d -> {
				Document copy = new Document(d);//Document.parse(d.toJson());
				String k = copy.remove("_id").toString();
				return "INSERT INTO " + table + "_JSON" + kind + "(ID,JSON) VALUES ('" + k + "','" + copy.toJson()
						+ "')";
			}).forEach(s -> {
				try {
					statement.addBatch(s);
				} catch (SQLException exc) {
					exc.printStackTrace();
				}
			});
		} else {
			E3_DocumentSet.getInstance().getByName(kind).stream().map(d ->
				"INSERT INTO " + table + "_TUPLE" + kind + "(ID,"+
						d.keySet().stream().filter(t->!t.equals("_id")).sorted().collect(Collectors.joining(","))
						+") VALUES ('" + d.getString("_id") + "',"
						+ d.keySet().stream().filter(t->!t.equals("_id")).sorted().map(k->d.get(k) == null ? "null" : "'"+d.get(k).toString()+"'").collect(Collectors.joining(","))
						+ ")"
			).forEach(s -> {
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
				kind.substring(kind.lastIndexOf("_") + 1), String.valueOf(1d - Math.pow(2, -probability)),
				String.valueOf(elapsedTime) });
	}

	public void sumTuple(String kind, boolean nullIsText) throws Exception {
//		ArrayList<String> attribs = Lists.newArrayList(
//				IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)))
//						.sorted().collect(Collectors.toList()));
		StringBuilder sb = new StringBuilder("SELECT  SUM( a01");
//		for (String string : attribs) {
//			if (nullIsText)	sb.append("case when ").append(string).append(" is null then 0 else ").append(string).append(" end +");
//			else sb.append(string).append(" +");
//		}
//
//		// remove the trailing +
//		if (sb.length() > 0)
//			sb.deleteCharAt(sb.length() - 1);

		sb.append(") FROM ").append(table + "_tuple" + kind);
		System.out.println(sb.toString());

		PreparedStatement stmt = JDBC.prepareStatement(sb.toString());

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres_TUPLE", "sum",
				kind.substring(kind.lastIndexOf("_") + 1), String.valueOf(1d - Math.pow(2, -probability)),
				String.valueOf(elapsedTime) });
	}

	public void sumJSON(String kind) throws Exception {
		ArrayList<String> attribs = Lists.newArrayList(
				IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)))
						.sorted().collect(Collectors.toList()));
		StringBuilder sb = new StringBuilder("SELECT  SUM(").append("(\"json\"->>'").append("a01").append("')::numeric +");

//		for (String string : attribs) {
//			sb.append("case when ").append("(\"json\"->>'").append(string).append("') is null then 0 else ")
//					.append("(\"json\"->>'").append(string).append("')::int end +");
//		}

		if (sb.length() > 0)
			sb.deleteCharAt(sb.length() - 1);

		sb.append(") FROM ").append(table + "_json" + kind);
		System.out.println(sb.toString());

		PreparedStatement stmt = JDBC.prepareStatement(sb.toString());

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres_JSON", "sum",
				kind.substring(kind.lastIndexOf("_") + 1), String.valueOf(1d - Math.pow(2, -probability)),
				String.valueOf(elapsedTime) });
	}

	public void countNulls(boolean isJSON, String kind) throws Exception {
		String sql = "";
		if (isJSON) {
			String expr;
			if (kind.equals("_NULLS_ARE_TEXT") || kind.equals("_NULLS_ARE_NOTHING"))
				expr = "json->>'a01' is null";
			else
				expr = "(json->>'a01')::numeric = 0";
			sql = "select count(*) from " + table + "_JSON" + kind + " where " + expr;
		} else {
			if (kind.equals("_NULLS_ARE_TEXT"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a01 is null";
			else if (kind.equals("_NULLS_ARE_ZERO"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a01=0";
		}
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "countNulls",
				kind.substring(kind.lastIndexOf("_") + 1), String.valueOf(1d - Math.pow(2, -probability)),
				String.valueOf(elapsedTime) });

	}

	public void countNotNulls(boolean isJSON, String kind) throws Exception {
		String sql = "";
		if (isJSON) {
			String expr;
			if (kind.equals("_NULLS_ARE_TEXT") || kind.equals("_NULLS_ARE_NOTHING"))
				expr = "json->>'a01' is not null";
			else
				expr = "(json->>'a01')::numeric <> 0";
			sql = "select count(*) from " + table + "_JSON" + kind + " where " + expr;
		} else {
			if (kind.equals("_NULLS_ARE_TEXT"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a01 is not null";
			else if (kind.equals("_NULLS_ARE_ZERO"))
				sql = "select count(*) from " + table + "_TUPLE" + kind + " where a01<>0";
		}
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "countNotNulls",
				kind.substring(kind.lastIndexOf("_") + 1), String.valueOf(1d - Math.pow(2, -probability)),
				String.valueOf(elapsedTime) });
	}

	public void size(boolean isJSON, String kind) throws SQLException {
		String sql = " SELECT  pg_total_relation_size('" + table + (isJSON ? "_JSON" : "_TUPLE") + kind
				+ "') ;";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { isJSON ? "Postgres_JSON" : "Postgres_TUPLE", "size",
				kind.substring(kind.lastIndexOf("_") + 1), String.valueOf(1d - Math.pow(2, -probability)), "",
				rs.getString(1) });
	}

	public void destroyme() {
		instance = null;
	}

}
