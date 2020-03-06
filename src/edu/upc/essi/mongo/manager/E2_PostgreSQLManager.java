package edu.upc.essi.mongo.manager;

import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.bson.Document;
import java.sql.*;

public class E2_PostgreSQLManager {

	private static E2_PostgreSQLManager instance = null;
	private static CSVWriter writer;
	private String table;
	private int nLevels;
	private int nAttributes;
	private Connection JDBC;

	public static E2_PostgreSQLManager getInstance(String table, int nLevels, int nAttributes, CSVWriter writer) throws Exception {
		if (instance == null)
			instance = new E2_PostgreSQLManager(table, nLevels, nAttributes, writer);
		return instance;
	}

	public E2_PostgreSQLManager(String table, int nLevels, int nAttributes, CSVWriter writer2) throws Exception {
		this.table = table;
		this.nLevels=nLevels;
		this.nAttributes=nAttributes;
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

		JDBC.createStatement().execute("CREATE TABLE " + table + "(ID CHAR(24), JSON JSONB)");
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
		writer.writeNext(new String[] { "Postgres", "insert", table, String.valueOf(nLevels),
				String.valueOf(nAttributes), String.valueOf(elapsedTime)});
	}

	public void sum() throws Exception {
		String path = "sum((json";
		for (int i = 1; i < nLevels; ++i) {
			path += "->'a"+(i<10?'0'+String.valueOf(i):i)+"'";
		}
		path += "->>'a"+(nAttributes<10?'0'+String.valueOf(nAttributes):nAttributes) + "'" + ")::int) ";

		String sql = "select "+path+" from " + table;
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		long startTime = System.nanoTime();
		ResultSet rs = stmt.executeQuery();
		rs.next();
		System.out.println(rs.getInt(1));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Postgres", "sum", table, String.valueOf(nLevels),
				String.valueOf(nAttributes),String.valueOf(elapsedTime) });
	}

	public void size() throws SQLException {
		String sql = " SELECT pg_size_pretty( pg_total_relation_size('" + table + "') );";
		System.out.println(sql);
		PreparedStatement stmt = JDBC.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		writer.writeNext(new String[] { "Postgres", "size", table, String.valueOf(nLevels), String.valueOf(nAttributes),
				"", rs.getString(1) });
	}

	public void destroyme() {
		instance = null;
	}

}
