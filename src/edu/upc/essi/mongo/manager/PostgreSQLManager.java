package edu.upc.essi.mongo.manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import edu.upc.essi.mongo.datagen.DocumentSet;

public class PostgreSQLManager {

	private static PostgreSQLManager instance = null;
	private static String table;
	private Connection JDBC;

	public static PostgreSQLManager getInstance(String table) throws Exception {
		if (instance == null)
			instance = new PostgreSQLManager(table);
		return instance;
	}

	public PostgreSQLManager(String table) throws ClassNotFoundException, SQLException {
		this.table = table;
		Class.forName("org.postgresql.Driver");
		JDBC = DriverManager.getConnection("jdbc:postgresql://localhost/ideas_experiments", "postgres", "postgres");
		JDBC.setAutoCommit(false);

		JDBC.createStatement().execute("DROP TABLE IF EXISTS "+table+"_JSON");JDBC.commit();
		JDBC.createStatement().execute("CREATE TABLE "+table+"_JSON (ID SERIAL, JSON JSONB)");JDBC.commit();
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

}
