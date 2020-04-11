package edu.upc.essi.mongo.datagen;

import java.io.StringReader;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;

import edu.upc.essi.mongo.exp.CSVUtils;
import org.bson.Document;

import javax.json.*;

public class Generator {

	private static Generator generator_instance = null;
	private List list64m;
	private int idIndex;

	// private constructor restricted to this class itself
	private Generator() {
		list64m = CSVUtils.fillIds("data/80-2m");
//		list64m = CSVUtils.fillIds("/root/ideas/ids/80-2m");
//		list64m = CSVUtils.fillIds("/var/lib/postgresql/80-64m");
		idIndex = 0;
		System.out.println("Ids loaded");
	}
	
	public void resetIndex() {
		idIndex = 0;
	}

	private Object getNextID() throws RuntimeException {
		if (idIndex == list64m.size())
			throw new RuntimeException("Exceeded IDs limit");
		return list64m.get(idIndex++);
	}

	// static method to create instance of Singleton class
	public static Generator getInstance() {
		if (generator_instance == null)
			generator_instance = new Generator();
		return generator_instance;
	}

	public List<Document> generateFromPseudoJSONSchema(int documentCount, String schemaPath)
			throws RuntimeException, Exception {
		JsonObject schema = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schemaPath).toUri())))
				.readObject();
		if (!schema.getString("type").equals("object"))
			throw new RuntimeException("The type of the root must be object");
		List<Document> out = Lists.newArrayList();//Json.createArrayBuilder();
		for (int i = 0; i < documentCount; ++i) {
			out.add(generateJSONObject(schema));
		}
		return out;//.build().stream().map(d -> toLongDocument(Document.parse(d.toString()))).collect(Collectors.toList());
		//return out.build();
	}

	private Document generateJSONObject(JsonObject schema) throws RuntimeException {
		Document out = new Document();//Json.createObjectBuilder();
		if (schema.containsKey("_id") && schema.getBoolean("_id"))
			out.append("_id", getNextID().toString());
		schema.getJsonObject("properties").keySet().forEach(prop -> {
			JsonObject property = schema.getJsonObject("properties").getJsonObject(prop);
			if (property.containsKey("nullProbability")
					&& Math.random() < property.getJsonNumber("nullProbability").doubleValue()) {
				out.append(prop, JsonValue.NULL);
			} else {
				switch (property.getString("type")) {
				case "long":
					out.append(prop,generateLong(property));
					break;
				case "number":
					out.append(prop, generateNumber(property));
					break;
				case "string":
					out.append(prop, generateString(property));
					break;
				case "object":
					out.append(prop, generateJSONObject(property));
					break;
				case "array":
					out.append(prop, generateJsonArray(property));
				}
			}
		});
		return out;//.build();
	}

	// from https://mkyong.com/java/java-generate-random-integers-in-a-range/
	private List<Object> generateJsonArray(JsonObject property) {
		List<Object> arr = Lists.newArrayList();//Json.createArrayBuilder();
		int howMany = new SplittableRandom().nextInt(
				(property.getJsonNumber("maxSize").intValue() - property.getJsonNumber("minSize").intValue()) + 1)
				+ property.getJsonNumber("minSize").intValue();
		switch (property.getJsonObject("contents").getString("type")) {
		case "long":
			for (int i = 0; i < howMany; ++i) {
				arr.add(generateLong(property.getJsonObject("contents")));
			}
			break;
		case "number":
			for (int i = 0; i < howMany; ++i) {
				arr.add(generateNumber(property.getJsonObject("contents")));
			}
			break;
		case "string":
			for (int i = 0; i < howMany; ++i) {
				arr.add(generateString(property.getJsonObject("contents")));
			}
			break;
		case "object":
			for (int i = 0; i < howMany; ++i) {
				arr.add(generateJSONObject(property.getJsonObject("contents")));
			}
			break;
		case "array":
			for (int i = 0; i < howMany; ++i) {
				arr.add(generateJsonArray(property.getJsonObject("contents")));
			}
			;
		}
		return arr;//.build();
	}

	private Double generateLong(JsonObject property) {
		if (property.containsKey("minimum") && property.containsKey("maximum"))
			return generateLong(property.getInt("minimum"), property.getInt("maximum"));
		else if (property.containsKey("minimum") && !property.containsKey("maximum"))
			return generateLong(property.getInt("minimum"), 1000);
		else if (!property.containsKey("minimum") && property.containsKey("maximum"))
			return generateLong(0, property.getInt("maximum"));
		else
			return generateLong(0, 1000);
	}

	private int generateNumber(JsonObject property) {
		if (property.containsKey("minimum") && property.containsKey("maximum"))
			return generateNumber(property.getInt("minimum"), property.getInt("maximum"));
		else if (property.containsKey("minimum") && !property.containsKey("maximum"))
			return generateNumber(property.getInt("minimum"), 1000);
		else if (!property.containsKey("minimum") && property.containsKey("maximum"))
			return generateNumber(0, property.getInt("maximum"));
		else
			return generateNumber(0, 1000);
	}

	// from
	// https://stackoverflow.com/questions/11743267/get-random-numbers-in-a-specific-range-in-java
	private int generateNumber(int lowerbound, int upperbound) {
		return new SplittableRandom().nextInt(upperbound - lowerbound + 1) + lowerbound;
	}

	private Double generateLong(int lowerbound, int upperbound) {
		return new SplittableRandom().nextDouble((double)upperbound - (double)lowerbound + Double.valueOf(1)) + (double)lowerbound;
	}

	private String generateString(JsonObject property) {
		if (property.containsKey("domain") && property.containsKey("size"))
			throw new RuntimeException("Cannot have both options domain and size");
		if (!property.containsKey("domain") && !property.containsKey("size"))
			throw new RuntimeException("At least must contain an option domain or size");
		if (property.containsKey("domain"))
			return generateStringFromDomain(property.getJsonArray("domain"));
		return generateStringWithSize(property.getJsonNumber("size").intValue());
	}

	// from https://www.baeldung.com/java-random-list-element
	private String generateStringFromDomain(JsonArray domain) {
		return domain.getString(new SplittableRandom().nextInt(domain.size()));
	}

	private String generateStringWithSize(int size) {
		return RandomStringUtils.randomAlphanumeric(size);
	}
}
