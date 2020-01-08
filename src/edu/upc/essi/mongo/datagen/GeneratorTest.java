package edu.upc.essi.mongo.datagen;

import org.json.simple.JSONArray;

public class GeneratorTest {

    public static void main(String args[]) {
        Generator gen = Generator.getInstance();
        JSONArray out = gen.generate(true,10, 3, 3, .5f, .2f, 0f);
        out.forEach(System.out::println);
    }
}
