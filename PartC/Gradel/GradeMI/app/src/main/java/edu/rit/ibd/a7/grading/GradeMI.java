package edu.rit.ibd.a7.grading;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GradeMI {
	
	private static final List<Document> settings = new ArrayList<>();
	
	static {
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,62,55,3135,36,852,206,301,83,270,940,112,21,469,24,75,205,223,475,560,289,66,73,31,1067,127,4,27,68,3987,4773,4832,404,8531,2477,2952,51,204,517,53,32,43,216,542,104,3089,120,794,346,23,\", \"b\": \"1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1212,1212,1212,1212,1212,1212,1212,1212,1212,1212,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"54575\"}, \"hu\": {\"$numberDecimal\": \"3.297003301871997979300565066318499\"}, \"hv\": {\"$numberDecimal\": \"3.806662431005569717839000428339565\"}, \"mi\": {\"$numberDecimal\": \"0.02850663107440411774354206913310710\"}, \"emi\": {\"$numberDecimal\": \"0.03191508533426438063862981094136139\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"263,176,414,630,118,77,706,532,62,964,1848,1580,19,15,2658,722,1291,55,464,20,1583,4,112,578,2120,461,755,135,1784,1367,122,1069,1017,132,596,1810,530,138,997,727,744,1671,1395,39,208,669,293,996,1026,535,\", \"b\": \"1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1212,1212,1212,1212,1212,1212,1212,1212,1212,1212,0,0,0,0,0,\", \"c\": \"6,6,7,8,7,8,7,6,5,4,5,4,2,3,6,6,6,6,6,4,5,2,3,6,7,7,8,7,8,9,8,8,9,7,6,5,6,3,3,4,4,5,6,7,8,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"54575\"}, \"hu\": {\"$numberDecimal\": \"3.857463994262495468403176436324784\"}, \"hv\": {\"$numberDecimal\": \"3.806662431005569717839000428339565\"}, \"mi\": {\"$numberDecimal\": \"0.02805589723464147611154840903636017\"}, \"emi\": {\"$numberDecimal\": \"0.03113532309336095858448712753485830\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,177,260,3484,104,572,1,289,47,338,4673,64,58,162,105,51,236,240,239,123,67,1577,361,205,131,112,0,14,69,4057,6188,1506,309,8205,3623,3540,271,53,454,117,35,169,212,20,78,433,127,460,55,107,\", \"b\": \"1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1212,1212,1212,1212,1212,1212,1212,1212,1212,1212,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"54575\"}, \"hu\": {\"$numberDecimal\": \"3.204350179309242725567366891646275\"}, \"hv\": {\"$numberDecimal\": \"3.806662431005569717839000428339565\"}, \"mi\": {\"$numberDecimal\": \"0.02525447232227270485931988512722168\"}, \"emi\": {\"$numberDecimal\": \"0.03244593434779820152980040745794897\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,234,185,836,336,965,574,311,340,421,700,33,121,1220,219,246,159,671,137,712,214,182,158,261,1570,206,139,528,900,1294,384,1105,1329,3076,718,3247,364,176,799,761,115,551,563,160,473,3860,182,111,425,82,\", \"b\": \"1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1213,1212,1212,1212,1212,1212,1212,1212,1212,1212,1212,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"54575\"}, \"hu\": {\"$numberDecimal\": \"3.848366048694418841001829457391008\"}, \"hv\": {\"$numberDecimal\": \"3.806662431005569717839000428339565\"}, \"mi\": {\"$numberDecimal\": \"0.02924718418399953630036740418250369\"}, \"emi\": {\"$numberDecimal\": \"0.03182863084724293383305063583430026\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,17,64,120,78,168,182,202,142,302,127,39,179,205,41,88,53,112,33,40,11,132,217,125,110,78,43,23,94,488,673,161,33,82,55,36,28,41,23,241,7,131,19,3,111,134,48,192,211,334,\", \"b\": \"136,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"6076\"}, \"hu\": {\"$numberDecimal\": \"3.500478205492306790416024674006134\"}, \"hv\": {\"$numberDecimal\": \"3.806661895252139117017133926809077\"}, \"mi\": {\"$numberDecimal\": \"0.1423015416355546294945802076733424\"}, \"emi\": {\"$numberDecimal\": \"0.1915147465632971620120274178394815\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"17,189,64,17,172,54,70,37,88,54,221,229,153,75,57,101,54,58,55,82,45,35,122,158,377,138,444,228,177,15,9,75,89,45,84,121,33,112,91,98,197,229,103,210,50,190,186,57,428,83,\", \"b\": \"136,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,1,1,2,2,2,2,2,2,2,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"6076\"}, \"hu\": {\"$numberDecimal\": \"3.632324042811596252922827927989866\"}, \"hv\": {\"$numberDecimal\": \"3.806661895252139117017133926809077\"}, \"mi\": {\"$numberDecimal\": \"0.1720623077293609842397997232555206\"}, \"emi\": {\"$numberDecimal\": \"0.2096027488071534875169156365442459\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,17,63,62,154,80,203,210,100,205,49,35,59,166,163,94,73,69,24,37,484,95,98,23,70,89,96,53,130,434,206,197,47,205,122,67,64,79,29,110,30,120,139,156,125,148,177,150,203,267,\", \"b\": \"136,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"6076\"}, \"hu\": {\"$numberDecimal\": \"3.656568388159148151036758000217589\"}, \"hv\": {\"$numberDecimal\": \"3.806661895252139117017133926809077\"}, \"mi\": {\"$numberDecimal\": \"0.1577969735572448392311493507098121\"}, \"emi\": {\"$numberDecimal\": \"0.2102462720498739669779122435790507\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,17,398,130,12,266,314,178,450,270,62,65,48,88,227,106,35,32,165,40,297,43,163,22,93,75,61,90,76,125,430,186,95,33,56,173,43,23,9,117,25,18,39,6,117,249,201,40,75,193,\", \"b\": \"136,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,135,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"6076\"}, \"hu\": {\"$numberDecimal\": \"3.521953364818800288154789299943965\"}, \"hv\": {\"$numberDecimal\": \"3.806661895252139117017133926809077\"}, \"mi\": {\"$numberDecimal\": \"0.1672608703356484257649145819578925\"}, \"emi\": {\"$numberDecimal\": \"0.1904995749756198720673992071760982\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,34,45,12,134,45,63,70,50,39,176,32,5,37,51,45,10,7,22,160,40,77,64,67,169,139,65,35,8,34,120,109,20,52,16,35,45,100,107,147,73,50,64,49,14,11,43,72,96,74,\", \"b\": \"149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,148,148,148,148,148,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"6700\"}, \"hu\": {\"$numberDecimal\": \"4.357890061825076567108173437389280\"}, \"hv\": {\"$numberDecimal\": \"3.806660258211836289335194180965003\"}, \"mi\": {\"$numberDecimal\": \"0.3114565578084822017803634553000806\"}, \"emi\": {\"$numberDecimal\": \"0.3554535543847066337862203288645438\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"18,163,44,19,129,28,6,54,7,231,64,245,2,4,5,8,1,3,2,14,66,197,201,18,230,44,55,44,2,5,53,33,50,46,55,94,99,95,182,53,10,5,69,134,41,31,15,133,6,26,\", \"b\": \"149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,148,148,148,148,148,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,1,1,1,1,1,2,2,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"6700\"}, \"hu\": {\"$numberDecimal\": \"4.140402805466050065579190841382821\"}, \"hv\": {\"$numberDecimal\": \"3.806660258211836289335194180965003\"}, \"mi\": {\"$numberDecimal\": \"0.2741752691218736352182310912413969\"}, \"emi\": {\"$numberDecimal\": \"0.3001137230063477519608285856059536\"}}")
					));
		
		settings.add(new Document().append("MaxMinutes", 5).append("Epoch", 5).
				append("Expected", Document.parse("{\"_id\": \"mi_info\", \"a\": \"0,64,37,10,99,27,74,81,36,61,133,34,15,57,40,68,26,136,10,115,66,64,69,133,8,13,72,68,7,3,8,133,28,60,33,35,10,108,58,174,133,24,39,43,20,14,32,66,87,65,\", \"b\": \"149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,149,148,148,148,148,148,0,0,0,0,0,\", \"c\": \"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\", \"n\": {\"$numberDecimal\": \"6700\"}, \"hu\": {\"$numberDecimal\": \"4.332052679566425970581256437560153\"}, \"hv\": {\"$numberDecimal\": \"3.806660258211836289335194180965003\"}, \"mi\": {\"$numberDecimal\": \"0.3008397539181273742858925931624430\"}, \"emi\": {\"$numberDecimal\": \"0.3487948338625776600019448460349964\"}}")
					));
	}


	public static void main(String[] args) {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String folderToJSONFiles = args[2];
		final String folderToAssignment = args[3];
		final boolean windows = Boolean.valueOf(args[4]);
		
		// Name of the folder containing the Gradle project.
		final String folderToSearch = "MutualInformation";
		// Maximum size in MB of the Gradle project without build.
		final double maxSize = 2048.0;
		// Maximum amount of main memory allowed in MB.
		final int maxMem = 32;
		
		MongoClient client = null;
		MongoDatabase db = null;
		
		// Get each of the folders in the directory. Each folder is a student's username, e.g., crrvcs.
		for (File studentFolder : new File(folderToAssignment).listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.isDirectory();
				}
			})) {
			
			if (studentFolder.getName().startsWith("_"))
				continue;
			
			System.out.println("Student: " + studentFolder.getName());
			try {
				// Search for the folder no further than three steps.
				File gradleProject = searchFolder(studentFolder, folderToSearch, 3);
				
				if (gradleProject != null) {
					File buildFolder = new File(gradleProject, "app/build");
					
					// Delete all previous builds first.
					if (buildFolder.exists())
						MoreFiles.deleteRecursively(Paths.get(buildFolder.toURI()), RecursiveDeleteOption.ALLOW_INSECURE);
					
					// Get total size and make sure it is within the requirements.
					long size = getFolderSize(gradleProject);
					if (size/1024.0 > maxSize) {
						System.out.println("The total size is " + size/1024.0 + " which is larger than expected.");
						continue;
					}
					
					// Build and install Gradle.
					File gradlewFile = new File(gradleProject.getAbsolutePath() + "/gradlew"+(windows?".bat":""));
					if (!windows)
						Files.setPosixFilePermissions(Paths.get(gradlewFile.toURI()), PosixFilePermissions.fromString("rwxrwxr-x"));
					
					boolean error = runProcess(new String[]{gradlewFile.getAbsolutePath(), "-p", gradleProject.getAbsolutePath(), "build"}, 5);
					error = error || runProcess(new String[]{gradlewFile.getAbsolutePath(), "-p", gradleProject.getAbsolutePath(), "installDist"}, 5);
					if (error) {
						System.out.println("Could not compile/install project.");
						continue;
					}
					
					for (int current = 0; current < settings.size(); current++) {
						Document setting = settings.get(current);
						String mongoCol = "Points_"+(current%12)+"_"+setting.get("Epoch");
						
						// Destroy the existing collection!
						client = getClient(mongoDBURL);
						db = client.getDatabase(mongoDBName);
						db.getCollection(mongoCol).drop();

						MongoCollection<Document> collection = db.getCollection(mongoCol);
						
						// Read JSON file with points and load into collection.
						Scanner scan = new Scanner(new File(folderToJSONFiles + mongoCol+".json"));
						while (scan.hasNextLine())
							collection.insertOne(Document.parse(scan.nextLine()));
						scan.close();
						
						// Label u will be the label assigned by K-Means.
						collection.updateMany(Document.parse("{_id:/p\\_.*/}"), 
								Lists.newArrayList(Document.parse("{$addFields : {label_u : {$toInt : {$replaceOne : {input:'$label', find:'c_', replacement:''}}}}}")));
						// Label v will be deterministic random.
						int totalPoints = 0, R = (int) collection.countDocuments(Document.parse("{_id:/c\\_.*/}")), C = 45;
						for (Document point : collection.find(Document.parse("{_id:/p\\_.*/}")).sort(Document.parse("{_id:"+((current%2==0?-1:1))+"}"))) {
							collection.updateOne(new Document().append("_id", point.getString("_id")), new Document().append("$set", new Document().append("label_v", totalPoints%C)));
							totalPoints++;
						}
						
						
						client.close();
						
						{
							Document expected = setting.get("Expected", Document.class);
						
							// Let's run the program!
							ProcessBuilder builder = new ProcessBuilder(gradleProject.getAbsolutePath() + 
									"/app/build/install/app/bin/app" + (windows?".bat":""), mongoDBURL, mongoDBName, mongoCol, ""+R, ""+(C+5)).redirectErrorStream(true);
							builder.environment().put("JAVA_OPTS", "-Xmx" + maxMem + "m");
					
							Process process = null;
							try {
								long before = System.nanoTime();
								// Start the process.
								process = builder.start();
								// Change false/true to print to console the output of the program.
								StreamGobbler gobbler = new GradeMI().new StreamGobbler(process.getInputStream(), false);
								// Start the gobbler to collect the console output if needed.
								gobbler.start();
								// We will wait a little bit...
								boolean done = process.waitFor(setting.getInteger("MaxMinutes"), TimeUnit.MINUTES);
								long after = System.nanoTime();
								double timeTaken = (after - before) / (1e9 * 3600);
								System.out.println("The process took " + timeTaken + " hours; ");
								
								// Not done, penalty and destroy process.
								if (!done) {
									System.out.println("The process did not run in the expected time.");
									System.out.println("\tPenalty: -5");
									destroyProcess(process);
								}
								
								// Let's check the results.
								client = getClient(mongoDBURL);
								db = client.getDatabase(mongoDBName);
								
								collection = db.getCollection(mongoCol);
								
								if (collection == null) {
									System.out.println("The expected collection did not exist.");
									System.out.println("\tPenalty: -15");
								} else {
									Document other = collection.find(new Document().append("_id", expected.get("_id"))).first();
									
									// Reduce arrays.
									for (String field : new String[] {"a", "b", "c"})
										if (other.containsKey(field)) {
											StringBuffer b = new StringBuffer();
											try {
												int total = 0;
												for (Decimal128 value : other.getList(field, Decimal128.class)) {
													b.append(value.intValue()+",");
													total++;
													
													// Only until 50!
													if (total == 50)
														break;
												}
											} catch (Exception oops) {
												b.append("Incorrect type, should be array of Decimal128");
											}
											other.put(field, b.toString());
										}
									
									if (other == null || !Maps.difference(expected, other).areEqual()) {
										System.out.println("Expected MI is: " + expected);
										System.out.println("MI retrieved: " + other);
										System.out.println("\tPenalty: -5");
									}
								}
								
								client.close();
							} catch (Exception oops) {
								System.out.println("A major problem happened!");
								oops.printStackTrace(System.out);
							} finally {
								destroyProcess(process);
							}
						}
					}
				} else
					System.out.println("Folder not found!");
			} catch (Throwable oops) {
				System.out.println("Something went really wrong!");
				oops.printStackTrace(System.out);
			}
			
			System.out.println();
			System.out.println();
		}

	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}
	
	// In bytes.
	public static long getFolderSize(File folder) throws Exception {
		AtomicLong size = new AtomicLong();
		Files.walk(folder.toPath()).forEach(f -> {
			File file = f.toFile();
			if (file.isFile()) {
				size.addAndGet(file.length());
			}
		});
		return size.get();
	}

	public static File searchFolder(File folder, String nameToSearch, int depth) throws Exception {
		List<Path> result;
		try (Stream<Path> pathStream = Files.find(folder.toPath(), depth,
				(p, basicFileAttributes) -> p.toFile().isDirectory() && p.toFile().getName().equals(nameToSearch))) {
			result = pathStream.collect(Collectors.toList());
		}
		if (result.size() > 1)
			throw new Error("Several folders found!");
		else if (result.isEmpty())
			return null;
		else
			return result.get(0).toFile();
	}
	
	public static boolean runProcess(String[] command, int waitInMin) {
		boolean ret = false;
		ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
		Process process = null;
		try {
			process = builder.start();
			StreamGobbler gobbler = new GradeMI().new StreamGobbler(process.getInputStream(), true);
			gobbler.start();
			ret = !process.waitFor(5, TimeUnit.MINUTES);
		} catch (Exception oops) {
			System.out.println("A major problem happened: " + oops.getMessage() + "; ");
			ret = true;
		} finally {
			builder = null;
			if (process != null)
				process.destroy();
		}
		return ret;
	}
	
	public static void destroyProcess(Process process) {
		if (process != null) {
			process.descendants().forEach(d -> {d.destroy();});
			process.destroy();
		}
	}
	
	public class StreamGobbler extends Thread {
		private InputStream is;
		private boolean print;

		public StreamGobbler(InputStream is, boolean print) {
			this.is = is;
			this.print = print;
		}

		public void run() {
			try {
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String line = null;
				while ((line = br.readLine()) != null)
					if (print)
						System.out.println(line);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}

}
