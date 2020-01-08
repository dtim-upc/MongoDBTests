package edu.upc.essi.mongo.datagen;

import org.json.simple.JSONArray;

public class GeneratorTest {

    public static void main(String args[]) {
        Generator gen = Generator.getInstance();
        JSONArray out = gen.generate(10,3,.5f);
        out.forEach(System.out::println);
    }
}
