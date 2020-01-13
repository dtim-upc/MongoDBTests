package edu.upc.essi.mongo.datagen;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

//import org.json.simple.JSONArray;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;

import edu.upc.essi.mongo.exp.Const;

public class JsonIO {
/*
	public static void writeFile(String filename, JSONArray jsons) {
		try (FileWriter file = new FileWriter(Const.JSON_LOC + filename)) {

			file.write(jsons.toJSONString());
			file.flush();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static JSONArray readFile(String filename) {
		JSONParser jsonParser = new JSONParser();
		try (FileReader reader = new FileReader(filename)) {
			// Read JSON file
			Object obj = jsonParser.parse(reader);
			JSONArray list = (JSONArray) obj;
			return list;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
 */
}
