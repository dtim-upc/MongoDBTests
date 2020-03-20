package edu.upc.essi.mongo.manager;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class JSONSchema {

    private static boolean isNumeric (String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (Exception exc) {
            return false;
        }
    }

    /**
     * withCheckConstraints indicates value validation (e.g., minimum, maximum or enums)
     */
    public static JsonObject generate_e4_JSONSchema(JsonObject template, boolean withCheckConstraints,
                                                boolean isMongoDB, int validatedAttributes) {
        JsonObjectBuilder out = Json.createObjectBuilder();
        if (template.containsKey("type") && template.getString("type").equals("string") &&
                template.containsKey("domain") && withCheckConstraints) {
            out.add("enum",template.getJsonArray("domain"));
        }
        else if (template.containsKey("type") && isMongoDB) out.add("bsonType",
                template.getString("type").equals("number") ? "int" : template.getString("type"));
        template.keySet().forEach(k -> {
            if (withCheckConstraints && (k.equals("minimum") || k.equals("maximum")))
                out.add(k, template.get(k));
            else {
                if (!k.equals("minimum") && !k.equals("maximum") &&
                        !k.equals("type") && !k.equals("domain") && !k.equals("nullProbability") && !k.equals("_id") &&
                        !k.equals("minSize") && !k.equals("maxSize") && !k.equals("size")) {
                    out.add(k, template.get(k));
                }
            }
        });
        //all properties are required for now
        if (template.containsKey("properties")) {
            JsonArrayBuilder required = Json.createArrayBuilder();
            JsonObjectBuilder properties = Json.createObjectBuilder();
            template.getJsonObject("properties").keySet().forEach(k-> {
                if (!isNumeric(k.substring(1)) || !(Integer.parseInt(k.substring(1)) > validatedAttributes)) {
                    required.add(k);
                    properties.add(k, generate_e4_JSONSchema(
                            template.getJsonObject("properties").getJsonObject(k),
                            withCheckConstraints, isMongoDB, validatedAttributes));
                }
            });
            out.add("required", required.build());
            out.add("properties", properties.build());
        }
        return out.build();
    }

    public static JsonObject generate_e6_JSONSchema(JsonObject template, boolean withCheckConstraints,
                                                    boolean isMongoDB, int validatedAttributes) {
        JsonObjectBuilder out = Json.createObjectBuilder();
        if (template.containsKey("type") && template.getString("type").equals("string") &&
                template.containsKey("domain") && withCheckConstraints) {
            out.add("enum",template.getJsonArray("domain"));
        }
        else if (template.containsKey("type") && isMongoDB) out.add("bsonType",
                template.getString("type").equals("number") ? "int" : template.getString("type"));
        template.keySet().forEach(k -> {
            if (withCheckConstraints && (k.equals("minimum") || k.equals("maximum")))
                out.add(k, template.get(k));
            else {
                if (!k.equals("minimum") && !k.equals("maximum") &&
                        !k.equals("type") && !k.equals("domain") && !k.equals("nullProbability") && !k.equals("_id") &&
                        !k.equals("minSize") && !k.equals("maxSize") && !k.equals("size")) {
                    out.add(k, template.get(k));
                }
            }
        });
        //all properties are required for now
        if (template.containsKey("properties")) {
            JsonArrayBuilder required = Json.createArrayBuilder();
            JsonObjectBuilder properties = Json.createObjectBuilder();
            template.getJsonObject("properties").keySet().forEach(k-> {
                required.add(k);
                properties.add(k, generate_e6_JSONSchema(
                        template.getJsonObject("properties").getJsonObject(k),
                        !isNumeric(k.substring(1)) || !(Integer.parseInt(k.substring(1)) > validatedAttributes),
                         isMongoDB, validatedAttributes));
            });
            out.add("required", required.build());
            out.add("properties", properties.build());
        }
        return out.build();
    }

}
