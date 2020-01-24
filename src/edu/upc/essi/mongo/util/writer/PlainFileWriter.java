package edu.upc.essi.mongo.util.writer;
import java.io.IOException;
import java.io.FileWriter;
import java.io.File; 
import java.io.BufferedWriter; 

public class PlainFileWriter { 
	FileWriter fw;
	private BufferedWriter bw;
	public void create(String filePath) throws IOException {
		File file =new File(filePath); 
		if(!file.exists()){
	    	   file.createNewFile();
	    }
		fw = new FileWriter(file,true);
		bw = new BufferedWriter(fw);
	}

	public void write(String content) throws IOException {
		bw.write(content);
	}

	public void flush() throws IOException { 
		bw.flush(); 
	}

	public void close() throws IOException {
		fw.close();
	}

}
