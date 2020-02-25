package edu.upc.essi.mongo.manager;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class JSONSchema {

    /**
     * withCheckConstraints indicates value validation (e.g., minimum, maximum or enums)
     */
    public static JsonObject generateJSONSchema(JsonObject template, boolean withCheckConstraints, boolean isMongoDB) {
        JsonObjectBuilder out = Json.createObjectBuilder();
        if (template.containsKey("type") && template.getString("type").equals("string") &&
                template.containsKey("domain") && withCheckConstraints) {
            out.add("enum",template.getJsonArray("domain"));
        }
        else if (template.containsKey("type") && isMongoDB) out.add("bsonType",
                template.getString("type").equals("number") ? "int" : template.getString("type"));
        template.keySet().forEach(k -> {
            if (withCheckConstraints && (k.equals("minimum") || k.equals("maximum")))
                out.add(k,template.get(k));
            else {
                if (!k.equals("minimum") && !k.equals("maximum") &&
                    !k.equals("type") && !k.equals("domain") && !k.equals("nullProbability") && !k.equals("_id") &&
                    !k.equals("minSize") && !k.equals("maxSize") && !k.equals("size")) {
                    out.add(k,template.get(k));
                }
            }
        });
        //all properties are required for now
        if (template.containsKey("properties")) {
            JsonArrayBuilder required = Json.createArrayBuilder();
            JsonObjectBuilder properties = Json.createObjectBuilder();
            template.getJsonObject("properties").keySet().forEach(k-> {
                required.add(k);
                properties.add(k, generateJSONSchema(
                        template.getJsonObject("properties").getJsonObject(k), withCheckConstraints, isMongoDB));
            });
            out.add("required", required.build());
            out.add("properties", properties.build());
        }
        return out.build();
    }
}
