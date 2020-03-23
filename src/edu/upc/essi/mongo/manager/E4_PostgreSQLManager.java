package edu.upc.essi.mongo.manager;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.bson.Document;

import javax.json.JsonObject;
import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class E4_PostgreSQLManager {

	private static E4_PostgreSQLManager instance = null;
	private static CSVWriter writer;
	private int attributes;
	private String table;
	private static JsonObject JSONSchema;
	private Connection JDBC;

	public static E4_PostgreSQLManager getInstance(String table, int attributes, JsonObject JSONSchema,
			CSVWriter writer) throws Exception {
		if (instance == null)
			instance = new E4_PostgreSQLManager(table, attributes, JSONSchema, writer);
		return instance;
	}
	
	public void reconnect() {
		try {
			JDBC = DriverManager.getConnection("jdbc:postgresql://10.55.0.32/ideas_experiments", "postgres", "user");
			JDBC.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public E4_PostgreSQLManager(String table, int attributes, JsonObject JSONSchema, CSVWriter writer2)
			throws Exception {
		this.table = table;
		this.attributes = attributes;
		this.JSONSchema = JSONSchema;
		this.writer = writer2;

		Class.forName("org.postgresql.Driver");
		// Drop and create DB

//		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres").createStatement().execute(""
//		+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
//		+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
//JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");

//		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "TYPsm3").createStatement().execute(""
//				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
//				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
//		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "TYPsm3");
		
		DriverManager.getConnection("jdbc:postgresql://10.55.0.32/", "postgres", "user").createStatement().execute(""
				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
		JDBC = DriverManager.getConnection("jdbc:postgresql://10.55.0.32/ideas_experiments", "postgres", "user");
		
		JDBC.setAutoCommit(false);
		JDBC.createStatement().execute("CREATE EXTENSION \"postgres-json-schema\"");
		JDBC.commit();

		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withVal (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement()
				.execute("ALTER TABLE " + table
						+ "_JSON_withVal ADD CONSTRAINT data_is_valid CHECK (validate_json_schema('"
						+ JSONSchema.toString() + "', JSON))");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withoutVal (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement()
				.execute("CREATE TABLE " + table + "_TUPLE (ID CHAR(24)," +
						IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)) + " int")
								.sorted().collect(Collectors.joining(",")) + ")");

		JDBC.commit();
	}

	public void insert(String kind) throws SQLException {
		Statement statement = JDBC.createStatement();
		if (kind.contains("JSON")) {
			DocumentSet.getInstance().documents.stream().map(d -> {
				Document copy = Document.parse(d.toJson());
				String k = copy.remove("_id").toString();
				return "INSERT INTO " + table + kind + " (ID,JSON) VALUES ('" + k + "','" + copy.toJson() + "')";
			}).forEach(s -> {
				try {
					statement.addBatch(s);
				} catch (SQLException exc) {
					exc.printStackTrace();
				}
			});
		} else {
			DocumentSet.getInstance().documents.stream()
					.map(d -> "INSERT INTO " + table + kind + " (ID," +
								IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)))
										.sorted().collect(Collectors.joining(",")) + ") VALUES ('"
								+ d.get("_id") + "',"
								+ d.keySet().stream().filter(k -> !k.equals("_id")).sorted()
								.map(k -> String.valueOf(d.getInteger(k))).collect(Collectors.joining(","))
								+ ")"
					)
					.forEach(s -> {
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
		writer.writeNext(new String[] { "Postgres", "insert", kind.substring(kind.lastIndexOf("_") + 1),
				table, String.valueOf(elapsedTime) });
	}

	public void sumTuple(String kind) throws Exception {
		ArrayList<String> attribs = Lists.newArrayList(
				IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)))
				.sorted().collect(Collectors.toList()));
		StringBuilder sb = new StringBuilder("SELECT  SUM(");
		for (String string : attribs) {
			sb.append(string + "+");
		}
		// remove the trailing +
		if (sb.length() > 0)
			sb.deleteCharAt(sb.length() - 1);

		sb.append(") FROM ").append(table + "_TUPLE");
		System.out.println(sb.toString());

		PreparedStatement stmt = JDBC.prepareStatement(sb.toString());

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", "sum", kind.substring(kind.lastIndexOf("_") + 1),
				table, String.valueOf(elapsedTime) });
	}

	public void sumJSON(String kind) throws Exception {
		ArrayList<String> attribs = Lists.newArrayList(
				IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)))
						.sorted().collect(Collectors.toList()));
		StringBuilder sb = new StringBuilder("SELECT  SUM(");

		for (String string : attribs) {
			sb.append("(\"json\"->>'").append(string).append("')::int+");
		}

		if (sb.length() > 0)
			sb.deleteCharAt(sb.length() - 1);

		sb.append(") FROM ").append(table + kind);
		System.out.println(sb.toString());

		PreparedStatement stmt = JDBC.prepareStatement(sb.toString());

		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", "sum", kind.substring(kind.lastIndexOf("_") + 1),
				table, String.valueOf(elapsedTime) });
	}

	public void size(String kind) throws SQLException {
		String sql = " SELECT  pg_total_relation_size('" + table + kind + "');";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { "Postgres", "size", kind.substring(kind.lastIndexOf("_") + 1),
				table, "", rs.getString(1) });
	}

	public void destroyme() {
		instance = null;
	}

}
