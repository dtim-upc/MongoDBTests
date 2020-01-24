package edu.upc.essi.mongo.util.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PlainFileReader{

	private BufferedReader br = null;
	private boolean hasNext = true;
	
	public void openConnection(String path) throws IOException {
		br=new BufferedReader(new FileReader(path));
	}

	public String read() { 
		String line=null;
		try {
			line = br.readLine();
			if(line==null) {
				hasNext=false;
			}
		} catch (IOException e) { 
			e.printStackTrace();
		}
		return line;
	}

	public boolean hasNext() { 
		return hasNext;
	}

	public void closeConnection() throws IOException {
		br.close();
	}

}
