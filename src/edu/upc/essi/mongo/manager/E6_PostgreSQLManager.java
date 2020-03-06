package edu.upc.essi.mongo.manager;

import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.bson.Document;

import javax.json.JsonObject;
import java.sql.*;
import java.util.stream.Collectors;

public class E6_PostgreSQLManager {

	private static E6_PostgreSQLManager instance = null;
	private static CSVWriter writer;
	private int attributes;
	private String table;
	private static JsonObject JSONSchema;
	private Connection JDBC;

	public static E6_PostgreSQLManager getInstance(String table, int attributes, JsonObject JSONSchema,
			CSVWriter writer) throws Exception {
		if (instance == null)
			instance = new E6_PostgreSQLManager(table, attributes, JSONSchema, writer);
		return instance;
	}

	public E6_PostgreSQLManager(String table, int attributes, JsonObject JSONSchema, CSVWriter writer2)
			throws Exception {
		this.table = table;
		this.attributes = attributes;
		this.JSONSchema = JSONSchema;
		this.writer = writer2;

		Class.forName("org.postgresql.Driver");
		// Drop and create DB

//		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres").createStatement().execute(""
//				+ "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); "
//				+ "drop database if exists ideas_experiments; " + "create database ideas_experiments;");
//		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");

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
				.execute("CREATE TABLE " + table + "_TUPLE (ID CHAR(24)," + JSONSchema.getJsonArray("required").stream()
						.map(t -> t.toString() + " int").collect(Collectors.joining(",")) + ")");
		JSONSchema.getJsonObject("properties").keySet().forEach(k -> {
			int min = JSONSchema.getJsonObject("properties").getJsonObject(k).getInt("minimum");
			int max = JSONSchema.getJsonObject("properties").getJsonObject(k).getInt("maximum");
			try {
				JDBC.createStatement().execute("ALTER TABLE " + table + "_TUPLE ADD CONSTRAINT validate" + k
						+ " CHECK (" + k + " BETWEEN " + min + " AND " + max + ")");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});

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
					.map(d -> "INSERT INTO " + table + kind + " (ID," + d.keySet().stream()
							.filter(k -> !k.equals("_id")).sorted().collect(Collectors.joining(",")) + ") VALUES ('"
							+ d.get("_id") + "',"
							+ d.keySet().stream().filter(k -> !k.equals("_id")).sorted()
									.map(k -> String.valueOf(d.getInteger(k))).collect(Collectors.joining(","))
							+ ")")
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
				String.valueOf(attributes), String.valueOf(elapsedTime) });
	}

	public void sum(String kind) throws Exception {
		String sql = "";
		if (kind.contains("JSON")) {
			String expr = "sum((json->>'a01')::int)";
			sql = "select " + expr + " from " + table + kind;
		} else {
			sql = "select sum(a01) from " + table + kind;
		}
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", "sum", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(attributes), String.valueOf(elapsedTime) });
	}

	public void size(String kind) throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + kind + "') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { "Postgres", "size", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(attributes), "", rs.getString(1) });
	}

	public void destroyme() {
		instance = null;
	}

}
