package edu.upc.essi.mongo.manager;

import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class E2_PostgreSQLManager {

	private static E2_PostgreSQLManager instance = null;
	private static CSVWriter writer;
	private String table;
	private String schema;
	private Connection JDBC;

	public static E2_PostgreSQLManager getInstance(String table, String schema, CSVWriter writer) throws Exception {
		if (instance == null)
			instance = new E2_PostgreSQLManager(table, schema, writer);
		return instance;
	}

	public E2_PostgreSQLManager(String table, String schema, CSVWriter writer2) throws Exception {
		this.table = table;
		this.schema = schema;
		this.writer = writer2;

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

		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withSiblings (ID CHAR(24), JSON JSONB)");
		JDBC.createStatement().execute("CREATE TABLE " + table + "_JSON_withoutSiblings (ID CHAR(24), JSON JSONB)");
		JDBC.commit();
	}

	public void insert(String tableKind) throws SQLException {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents.stream().map(d -> {
			Document copy = Document.parse(d.toJson());
			String k = copy.remove("_id").toString();
			return "INSERT INTO " + table + tableKind + "(ID,JSON) VALUES ('" + k + "','" + copy.toJson() + "')";
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
		//writer.writeNext(new String[] { "Postgres", "insert", "JSONWithArray", String.valueOf(elapsedTime) });
	}

	public void sum(int nLevels, int nAttributes, String tableKind) throws Exception {
		String path = "sum((json";
		for (int i = 1; i < nLevels; ++i) {
			path += "->'a"+(i<10?'0'+String.valueOf(i):i)+"'";
		}
		path += "->>'a"+(nAttributes<10?'0'+String.valueOf(nAttributes):nAttributes) + "'" + ")::int) ";

		String sql = "select "+path+" from " + table + tableKind;
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		//writer.writeNext(new String[] { "Postgres", "sum", "JSONWithArray", String.valueOf(elapsedTime) });
	}

	public void size(String tableKind) throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + tableKind + "') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { "Postgres", "size", tableKind, "", rs.getString(1) });
	}

}
