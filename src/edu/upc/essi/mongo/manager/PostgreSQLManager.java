package edu.upc.essi.mongo.manager;

import java.io.StringReader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

import edu.upc.essi.mongo.datagen.DocumentSet;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import javax.json.Json;
import javax.json.JsonObject;

public class PostgreSQLManager {

	private static PostgreSQLManager instance = null;
	private String table;
	private String experiment_ID;
	private String schema;
	private Connection JDBC;

	public static PostgreSQLManager getInstance(String table, String experiment_ID, String schema) throws Exception {
		if (instance == null)
			instance = new PostgreSQLManager(table, experiment_ID, schema);
		return instance;
	}

	public PostgreSQLManager(String table, String experiment_ID, String schema) throws Exception {
		this.table = table;
		this.experiment_ID = experiment_ID;
		this.schema = schema;

		Class.forName("org.postgresql.Driver");
		//Drop and create DB
		DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres")
				.createStatement().execute("" +
				"SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ideas_experiments' AND pid <> pg_backend_pid(); " +
				"drop database if exists ideas_experiments; " +
				"create database ideas_experiments;");

		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");
		JDBC.setAutoCommit(false);

		JDBC.createStatement().execute("DROP TABLE IF EXISTS "+table+"_JSON");JDBC.commit();
		JDBC.createStatement().execute("CREATE TABLE "+table+"_JSON (ID SERIAL, JSON JSONB)");JDBC.commit();

		JDBC.createStatement().execute("DROP TABLE IF EXISTS "+table);JDBC.commit();
		if (experiment_ID.equals("e1")) {
			JDBC.createStatement().execute("CREATE TABLE "+table+" (ID SERIAL,"+getAttributesForE1(true)+")");JDBC.commit();
		} else {
			throw new Exception("Experiment not implemented yet");
		}
	}

	public void insertAsJSON() throws SQLException {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents
				.stream().map(d -> "INSERT INTO "+table+"_JSON(JSON) VALUES ('"+d.toJson()+"')")
				.forEach(s -> {
					try {
						statement.addBatch(s);
					} catch (SQLException exc) {
						exc.printStackTrace();
					}
				});
		statement.executeBatch();
		JDBC.commit();
	}

	public void insertAsTuple() throws Exception {
		Statement statement = JDBC.createStatement();
		DocumentSet.getInstance().documents
				.stream().map(d -> {
			try {
				return "INSERT INTO "+table+"("+getAttributesForE1(false)+") VALUES ("+getValuesForE1(d)+")";
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


	public String getAttributesForE1(boolean withTypes) throws Exception {
		String out = "";
		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
		int size = obj.getJsonObject("properties").getJsonObject("theArray").getInt("maxSize");
		boolean first = true;
		for (int i = 0; i < size; ++i) {
			out += "a"+i;
			if (withTypes) {
				out += " int";
			}
			out += ",";
		}
		return out.substring(0,out.length()-1);
	}

	public String getValuesForE1(Document d) {
		return d.getList("theArray",Integer.class).stream().map(i->String.valueOf(i)).collect(Collectors.joining(","));
	}



}
