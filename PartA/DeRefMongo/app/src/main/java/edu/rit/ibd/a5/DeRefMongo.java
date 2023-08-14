package edu.rit.ibd.a5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

public class DeRefMongo {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String jsonFile = args[2];

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		// TODO 0: Your code here!
		/*
		 * Read the MovieInfo file that contains a number of JSON documents. For each document, find a document in the 
		 * 	Movies collection that matches using de-referencing. Update the document in MongoDB by including the expected 
		 * 	field(s) with appropriate type.
		 *
		 * Note that the ids of the documents in the MovieInfo file are the IMDB ids, e.g., ttXYZ, so you must use XYZ.
		 *
		 * A document d in the Movies collection has d.id-conflicts++ if there exists another document x in the MovieInfo 
		 * 	file such that d._id == x.id and d.bechdel-test-id != x.resultLabel.
		 * 
		 * A document d in the Movies collection has d.title-conflicts++ if there exists another document x in the MovieInfo 
		 * 	file such that d.otitle = x.title and d._id != x.id (regardless of the x.resultLabel value).
		 */
		
		List<List<String>> movieInfoDocs = new ArrayList<>();
		File fileToRead = new File(jsonFile);

		BufferedReader FileReader= new BufferedReader(new FileReader(fileToRead));
		String line;
		while(FileReader.readLine() != null){
			line=FileReader.readLine();
			movieInfoDocs.add(Collections.singletonList(line));
		}

//		for(int i=0;i<= movieInfoDocs.size();i++){
//			Document vals=movieInfoDocs.get(i);
//
//			String lines= Files.readLines();
//
//		}
//
//		for (Document d : movieInfoDocs){
//			Document values=new Document();
//			for (String field: )
//		}

		// Read the file and load the docs in the list. Alternatively, you can also load them in a new collection in MongoDB.

		try{

			movieInfoDocs.add(readDocToList(fileToRead));
//			listOfDocs.forEach(movieInfoDocs.add());

		}catch(IOException e)
		{
			e.printStackTrace();
		}

		// resultLabel is the result of the Bechdel test (passes/fails) for a given movie. When you are dereferencing using 
		//	id, you should set the bechdel-test-id field in the database. When you are dereferencing using title, you should
		//	set the bechdel-test-title field in the database.

		boolean resultLabel = false;

		
		// When dereferencing using id:

		//	Let dInDB be a document in the database, and dInFile be a document in the file such that dInDB._id == dInFile._id.


		//	If dInDB.bechdel-test-id != dInFile.resultLabel, then dInDB.id-conflicts++.
		
		// When dereferencing using title:
		//	Let dInDB be a document in the database, and dInFile be a document in the file such that dInDB.otitle == dInFile.title.
		//	If dInDB._id != dInFile._id, then dInDB.title-conflicts++. Let otherDInDB be another document in the database such that 
		//		otherDInDB._id=dInFile._id, then otherDInDB.title-conflicts++.



		client.close();
	}

	private static List<String> readDocToList(File fileToRead) throws IOException{

		List<String> documentData=new ArrayList<>();
		BufferedReader bufReader=null;

		try{
			bufReader= new BufferedReader(new FileReader(fileToRead));

			String line;
			while((line= (bufReader.readLine())) != null ){
				documentData.add(line);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(bufReader != null){
				bufReader.close();
			}
		}
		return documentData;
	}

	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
