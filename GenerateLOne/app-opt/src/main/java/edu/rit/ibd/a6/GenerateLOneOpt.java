package edu.rit.ibd.a6;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GenerateLOneOpt {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColL1 = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> l1 = db.getCollection(mongoColL1);
		
		// TODO Your code here!
		


// TODO Your code here!

		/*
		 *
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 *
		 *
		 * You need to compose the new documents to be inserted in the L1 collection as {items: {pos_0:iid}, count:z}.
		 *
		 */

//		System.out.println(transactions.find(new Document("_id",1000)).first());
//		FindIterable<Document>  iterateTrans= transactions.find();
//		Iterator iter = iterateTrans.iterator();
//		while(iter.hasNext()){
//			System.out.println(iter.next());
//		}


		Iterable<Document> result = transactions.aggregate(Arrays.asList(new Document("$unwind",
						new Document("path", "$items")),
				new Document("$group",
						new Document("_id", "$items")
								.append("count",
										new Document("$sum", 1L))),
				new Document("$set",
						new Document("items",
								new Document("pos_0", "$_id"))),
				new Document("$project",
						new Document("_id", 0L)),
				new Document("$match",
						new Document("count",
								new Document("$gte", minSup)))));


		for( Document docVals : result ){
//			System.out.println(docVals);
//			int count= (int) docVals.get("count");
//			Document items = (Document) docVals.get("items");
////			int pos_0=docVals.getInteger("items");
//			Document doc = (Document) docVals.get("items");
//			int pos_0= (int) doc.get("pos_0");
//			Document docs= new Document() ;
//			docs.put("count",count);
//			docs.put("items", new Document().append("pos_0",222));
//			System.out.println(pos_0);

			int  count = ((Long)docVals.get("count")).intValue();
//			Object items = docVals.get("items");
			Document items = (Document) docVals.get("items");
			int pos_01 = (int) items.get("pos_0");

			Document addDoc= new Document();
			addDoc.append("count",count);
			Document itemsDoc = new Document();
			itemsDoc.append("pos_0",pos_01);

			addDoc.append("items",itemsDoc);

//			System.out.println(itemsDoc.get("pos_0"));
//			System.out.println(pos_01);

//			System.out.println(count);

			l1.insertOne(addDoc);

		}
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 * 
		 * Keep track of the transactions associated to each item using an array field named 'transactions'. Also, use _ids such that
		 * 	they reflect the lexicographical order in which documents are processed.
		 * 
		 */
		
		// TODO End of your code!
		
		client.close();
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
