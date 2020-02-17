package edu.upc.essi.mongo.datagen;

import com.google.common.collect.Lists;
import org.bson.Document;

import java.util.List;

public class E3_DocumentSet {

	private static E3_DocumentSet instance = null;

	public List<Document> documents_NULLS_ARE_TEXT;
	public List<Document> documents_NULLS_ARE_NOTHING;
	public List<Document> documents_NULLS_ARE_ZERO;

	public List<Document> getByName(String name) {
		if (name.equals("_NULLS_ARE_TEXT")) return documents_NULLS_ARE_TEXT;
		if (name.equals("_NULLS_ARE_NOTHING")) return documents_NULLS_ARE_NOTHING;
		else return documents_NULLS_ARE_ZERO;
	}

	public static E3_DocumentSet getInstance() {
		if (instance == null)
			instance = new E3_DocumentSet();
		return instance;
	}

	public E3_DocumentSet() {
		documents_NULLS_ARE_TEXT = Lists.newArrayList();
		documents_NULLS_ARE_NOTHING = Lists.newArrayList();
		documents_NULLS_ARE_ZERO = Lists.newArrayList();
	}

}
