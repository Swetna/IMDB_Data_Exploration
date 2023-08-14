/**
 * Progrom to create and build Relational database models using JDBC.
 * @st9252-Swetna Annette Sarang Tribhuvan
 * Professor- Carlos Rivero
 */

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class IMDBToSQL {
	private static final String NULL_STR = "\\N";

	public static int findInArray(String[] arrayToSearch, String value) {
		int foundVal = 0;
		for (int i = 0; i < arrayToSearch.length; i++) {
			if (arrayToSearch[i] == value)
				foundVal = i + 1;
		}
		return foundVal;
	}


	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String folderToIMDBGZipFiles = args[3];

		PreparedStatement st = null;

		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);

		PreparedStatement st0 = con.prepareStatement("Drop table IF EXISTS MovieGenre,Director,KnownFor,Producer,Actor,Writer,Person,Genre,Movies");
		st0.execute();
		st0.close();


		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS Movies" +
						"( id INT(8) NOT NULL," +
						"ptitle VARCHAR(100)," +
						"otitle VARCHAR(100)," +
						"adult INT," +
						"year INT(4)," +
						"runtime INT," +
						"rating FLOAT," +
						"totalvotes INT(7)," +
						"Primary key (id))");
		st.execute();
		st.close();


		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS Genre" +
						"( id INT NOT NULL," +
						"name VARCHAR(20)," +
						"Primary Key (id))");
		st.execute();
		st.close();


		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS MovieGenre" +
						"( mid INT NOT NULL," +
						"gid INT NOT NULL," +
						"Foreign Key(mid) REFERENCES Movies(id)," +
						"Foreign Key(gid) REFERENCES Genre(id))");
		st.execute();
		st.close();


		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS Person" +
						"( id INT NOT NULL," +
						"name VARCHAR(20) NOT NULL," +
						"byear INT," +
						"dyear INT," +
						"Primary Key (id))");
		st.execute();
		st.close();


		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS Actor" +
						"( mid INT NOT NULL," +
						"pid INT NOT NULL," +
						"Foreign Key(mid) REFERENCES Movies(id)," +
						"Foreign Key(pid) REFERENCES Person(id))");
		st.execute();
		st.close();


		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS Director" +
						"( mid INT NOT NULL," +
						"pid INT NOT NULL," +
						"Foreign Key(mid) REFERENCES Movies(id)," +
						"Foreign Key(pid) REFERENCES Person(id))");
		st.execute();
		st.close();

		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS Writer" +
						"( mid INT NOT NULL," +
						"pid INT NOT NULL," +
						"Foreign Key(mid) REFERENCES Movies(id)," +
						"Foreign Key(pid) REFERENCES Person(id))");
		st.execute();
		st.close();


		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS Producer" +
						"( mid INT NOT NULL," +
						"pid INT NOT NULL," +
						"Foreign Key(mid) REFERENCES Movies(id)," +
						"Foreign Key(pid) REFERENCES Person(id))");
		st.execute();
		st.close();

		st = con.prepareStatement
				("CREATE TABLE IF NOT EXISTS KnownFor" +
						"( mid INT NOT NULL," +
						"pid INT NOT NULL," +
						"Foreign Key(mid) REFERENCES Movies(id)," +
						"Foreign Key(pid) REFERENCES Person(id))");
		st.execute();
		st.close();


		//----Java program to read file-------//
		InputStream gzipStream = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		Scanner sc1 = new Scanner(gzipStream, "UTF-8");
		int cnt = 1;
		while (sc1.hasNextLine() && cnt < 1) {
			String line = sc1.nextLine();
			line = line.substring(2);
			System.out.println(line);
			cnt++;

		}
		String line0 = sc1.nextLine();
		System.out.println(line0);
		while (sc1.hasNextLine()) {
			String line = sc1.nextLine();
			line = line.substring(2);
			line = line.replaceAll(("^0+(?!$)"), "");
			System.out.println(line);
			cnt++;

		}
		sc1.close();


		con.setAutoCommit(false);



		PreparedStatement stLoad = null;
		//int cnt = 0;
		int step = 20;


		//-----Loading For ratings---------//
		InputStream gzipStreamRate = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.ratings.tsv.gz"));
		Scanner scRate = new Scanner(gzipStreamRate, "UTF-8");

		String line0Rate = scRate.nextLine();
		line0Rate = line0Rate.replaceAll(("titleType"), "");
//		System.out.println(line0);
		while (scRate.hasNextLine() && cnt <= 20) {
			String lineRate = scRate.nextLine();
			lineRate = lineRate.substring(2);
			lineRate = lineRate.replaceAll(("^0+(?!$)"), "");
//			System.out.println(lineRate);
			cnt++;
			// Split the line.
			String[] splitLineRate = lineRate.split("\\t+");
//			System.out.println(splitLineRate[1]);


			//---------- Load movies. & Genre -----------//
			InputStream gzipStreamM = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"));
			Scanner sc = new Scanner(gzipStreamM, "UTF-8");

			HashSet<String> genre = new HashSet();

//		stLoad = con.prepareStatement("INSERT INTO Movie(id, ptitle,otitle) VALUES(?,?,?)");
			String line0Movies = sc.nextLine();
			line0Movies = line0Movies.replaceAll(("titleType"), "");
//			System.out.println(line0Movies);


			while (sc.hasNextLine() && cnt <= 25) {
				String lineMovies = sc.nextLine();
				lineMovies = lineMovies.substring(2);
				lineMovies = lineMovies.replaceAll(("^0+(?!$)"), "");
				lineMovies = lineMovies.replaceAll(("short"), "");
//				System.out.println(line);
				cnt++;
				// Split the line.
				String[] splitLineMovies = lineMovies.split("\\t+");

//			System.out.println(splitLine[1]);

				PreparedStatement stLoadMovies = null;
				stLoadMovies = con.prepareStatement("INSERT IGNORE INTO Movies(id,ptitle, otitle,adult,year,runtime,rating,totalvotes) VALUES(?,?,?,?,?,?,?,?)");
				stLoadMovies.setInt(1, Integer.parseInt(splitLineMovies[0]));

				// Set title and account for null; note the second argument of setNull.
				if (splitLineMovies[1].equals(NULL_STR))
					stLoadMovies.setNull(2, Types.VARCHAR);
				else
					stLoadMovies.setString(2, splitLineMovies[1]);

				if (splitLineMovies[2].equals(NULL_STR))
					stLoadMovies.setNull(3, Types.VARCHAR);
				else
					stLoadMovies.setString(3, splitLineMovies[2]);

				if (splitLineMovies[3].equals(NULL_STR))
					stLoadMovies.setNull(4, Types.INTEGER);
				else
					stLoadMovies.setInt(4, Integer.parseInt(splitLineMovies[3]));

				if (splitLineMovies[4].equals(NULL_STR))
					stLoadMovies.setNull(5, Types.INTEGER);
				else
					stLoadMovies.setInt(5, Integer.parseInt(splitLineMovies[4]));


				if (splitLineMovies[5].equals(NULL_STR))
					stLoadMovies.setNull(6, Types.INTEGER);
				else
					stLoadMovies.setInt(6, Integer.parseInt(splitLineMovies[5]));


				if (splitLineRate[1].equals(NULL_STR))
					stLoadMovies.setNull(7, Types.FLOAT);
				else
					stLoadMovies.setFloat(7, Float.parseFloat(splitLineRate[1]));

				if (splitLineRate[2].equals(NULL_STR))
					stLoadMovies.setNull(8, Types.INTEGER);
				else
					stLoadMovies.setInt(8, Integer.parseInt((splitLineRate[2])));


				//Genre
				genre.add((splitLineMovies[7]));
				//System.out.println(genre);

				String[] genreArray = new String[genre.size()];
				genre.toArray(genreArray);


				PreparedStatement stLoadMovieGenre = null;
				stLoadMovieGenre = con.prepareStatement("INSERT IGNORE INTO MovieGenre(mid,gid) VALUES(?,?)");
				stLoadMovieGenre.setInt(1, Integer.parseInt(splitLineMovies[0]));
				int foundValue = findInArray(genreArray, splitLineMovies[7]);
				stLoadMovieGenre.setInt(2, foundValue);


				stLoadMovies.addBatch();
				stLoadMovieGenre.addBatch();

				if (cnt % step == 0) {
					stLoadMovies.executeBatch();
					stLoadMovieGenre.executeBatch();
					con.commit();
				}

				// Leftovers.
				stLoadMovies.executeBatch();
				stLoadMovieGenre.executeBatch();
				con.commit();
				stLoadMovies.close();
				stLoadMovieGenre.close();


				int countGenre = 1;
				PreparedStatement stLoadGenre = null;
				for (String temp : genre) {
					stLoadGenre = con.prepareStatement("INSERT IGNORE INTO Genre(id,name) VALUES(?,?)");
					stLoadGenre.setInt(1, countGenre);
					countGenre++;
					if (temp.equals(NULL_STR))
						stLoadGenre.setNull(2, Types.INTEGER);
					else
						stLoadGenre.setString(2, temp);
				}

				stLoadGenre.addBatch();

				if (/* Batch processing */ cnt <= 20) {
					stLoadGenre.executeBatch();
					con.commit();
				}


				// Leftovers.
				stLoadGenre.executeBatch();
				con.commit();
				stLoadGenre.close();

			}
			sc.close();
		}


//			String QUERY = "SELECT id, ptitle , otitle ,adult,year,runtime,rating,totalvotes FROM Movies";
//			PreparedStatement pstmt_movies = con.prepareStatement(QUERY);
//			ResultSet rstMovie = pstmt_movies.executeQuery();
//			System.out.println("Id\t\tPTitle\t\toTitle\t\tadult\t\tyear\t\truntime\t\trating\t\ttotalvotes\n");
//			while (rstMovie.next()) {
//				System.out.print(rstMovie.getInt(1));
//				System.out.print("\t\t" + rstMovie.getString(3));
//				System.out.print("\t\t" + rstMovie.getString(4));
//				System.out.print("\t\t" + rstMovie.getString(5));
//				System.out.print("\t\t" + rstMovie.getString(6));
//				System.out.print("\t\t" + rstMovie.getString(7));
//				System.out.print("\t\t" + rstMovie.getString(8));
//				System.out.println();
//
//
//			}
//
//			String QUERY_Genre = "SELECT id,name FROM Genre";
//			PreparedStatement pstmt_genre = con.prepareStatement(QUERY_Genre);
//			ResultSet rstGenre = pstmt_genre.executeQuery();
//			System.out.println("Id\t\tName\n");
//			while (rstGenre.next()) {
//				System.out.print(rstGenre.getInt(1));
//				System.out.print("\t\t" + rstGenre.getString(2));
//				System.out.println();
//
//
//			}
//
//				String QUERY_MovieGenre = "SELECT mid,gid FROM MovieGenre";
//				PreparedStatement pstmt_Moviegenre = con.prepareStatement(QUERY_MovieGenre);
//				ResultSet rstMovieGenre = pstmt_Moviegenre.executeQuery();
//				System.out.println("MId\t\tGID\n");
//				while (rstMovieGenre.next()) {
//					System.out.print(rstMovieGenre.getInt(1));
//					System.out.print("\t\t" + rstMovieGenre.getInt(2));
//					System.out.println();
//				}


		//-----------Actor-----------//
		InputStream gzipStreamPrincipals = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		Scanner sc = new Scanner(gzipStreamPrincipals, "UTF-8");
		st = con.prepareStatement("INSERT IGNORE INTO Actor(mid,pid) VALUES(?,?)");
		sc.nextLine();
		cnt = 0;
		while (sc.hasNextLine()) {
			String linePrincipal = sc.nextLine();
			linePrincipal = linePrincipal.substring(2);
			linePrincipal = linePrincipal.replaceAll(("^0+(?!$)"), "");
//			// Split the line.
			String[] splitLineCategory = linePrincipal.split("\t+");
			splitLineCategory[2] = splitLineCategory[2].substring(2);
			splitLineCategory[2] = splitLineCategory[2].replaceAll(("^0+(?!$)"), "");
			cnt++;

			if (splitLineCategory[3].equals("actor") || splitLineCategory[3].equals("self")) {
				st.setInt(1, Integer.parseInt(splitLineCategory[0]));
				st.setInt(2, Integer.parseInt(splitLineCategory[2]));

				st.addBatch();
			}
			if (cnt % step == 0) {
				st.executeBatch();
				con.commit();
			}
		}
		st.executeBatch();
		con.commit();
		sc.close();




		String QUERY_Actor = "SELECT mid,pid FROM Actor";
		PreparedStatement pstmt_Actor = con.prepareStatement(QUERY_Actor);
		ResultSet rstActor = pstmt_Actor.executeQuery();
		System.out.println("MId\t\tPId\n");
		while (rstActor.next()) {
			System.out.print(rstActor.getInt(1));
			System.out.print("\t\t" + rstActor.getInt(2));
			System.out.println();
		}


		//-----------Director-----------//
		InputStream gzipStreamDirector = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		sc = new Scanner(gzipStreamDirector, "UTF-8");
		st = con.prepareStatement("INSERT IGNORE INTO Director(mid,pid) VALUES(?,?)");
		sc.nextLine();
		cnt = 0;
		while (sc.hasNextLine()) {
			String lineDirector = sc.nextLine();
			lineDirector = lineDirector.substring(2);
			lineDirector = lineDirector.replaceAll(("^0+(?!$)"), "");
//			// Split the line.
			String[] splitLineDiretor = lineDirector.split("\t+");
			splitLineDiretor[2] = splitLineDiretor[2].substring(2);
			splitLineDiretor[2] = splitLineDiretor[2].replaceAll(("^0+(?!$)"), "");
			cnt++;

			if (splitLineDiretor[3].equals("director")){
				st.setInt(1, Integer.parseInt(splitLineDiretor[0]));
				st.setInt(2, Integer.parseInt(splitLineDiretor[2]));

				st.addBatch();
			}
			if (cnt % step == 0) {
				st.executeBatch();
				con.commit();
			}
		}
		st.executeBatch();
		con.commit();
		sc.close();




		String QUERY_Director = "SELECT mid,pid FROM Director";
		PreparedStatement pstmt_Director = con.prepareStatement(QUERY_Director);
		ResultSet rstDirector = pstmt_Director.executeQuery();
		System.out.println("MId\t\tPId\n");
		while (rstDirector.next()) {
			System.out.print(rstDirector.getInt(1));
			System.out.print("\t\t" + rstDirector.getInt(2));
			System.out.println();
		}


		//------------Producer--------------//
		InputStream gzipStreamProducer = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		sc = new Scanner(gzipStreamProducer, "UTF-8");
		st = con.prepareStatement("INSERT IGNORE INTO Producer(mid,pid) VALUES(?,?)");
		sc.nextLine();
		cnt = 0;
		while (sc.hasNextLine()) {
			String lineProducer = sc.nextLine();
			lineProducer = lineProducer.substring(2);
			lineProducer = lineProducer.replaceAll(("^0+(?!$)"), "");
//			// Split the line.
			String[] splitLineCategory = lineProducer.split("\t+");
			splitLineCategory[2] = splitLineCategory[2].substring(2);
			splitLineCategory[2] = splitLineCategory[2].replaceAll(("^0+(?!$)"), "");
			cnt++;

			if (splitLineCategory[3].equals("producer")) {
				st.setInt(1, Integer.parseInt(splitLineCategory[0]));
				st.setInt(2, Integer.parseInt(splitLineCategory[2]));

				st.addBatch();
			}
			if (cnt % step == 0) {
				st.executeBatch();
				con.commit();
			}
		}
		st.executeBatch();
		con.commit();
		sc.close();




		String QUERY_Producer = "SELECT mid,pid FROM Actor";
		PreparedStatement pstmt_Producer = con.prepareStatement(QUERY_Producer);
		ResultSet rstProducer = pstmt_Producer.executeQuery();
		System.out.println("MId\t\tPId\n");
		while (rstProducer.next()) {
			System.out.print(rstProducer.getInt(1));
			System.out.print("\t\t" + rstProducer.getInt(2));
			System.out.println();
		}


		//---Writer------//
		InputStream gzipStreamWriters = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		sc = new Scanner(gzipStreamWriters, "UTF-8");
		st = con.prepareStatement("INSERT IGNORE INTO Writer(mid,pid) VALUES(?,?)");
		sc.nextLine();
		cnt = 0;
		while (sc.hasNextLine()) {
			String linePrincipal = sc.nextLine();
			linePrincipal = linePrincipal.substring(2);
			linePrincipal = linePrincipal.replaceAll(("^0+(?!$)"), "");
			// Split the line.
			String[] splitLineCategory = linePrincipal.split("\t+");
			splitLineCategory[2] = splitLineCategory[2].substring(2);
			splitLineCategory[2] = splitLineCategory[2].replaceAll(("^0+(?!$)"), "");
			cnt++;

			if (splitLineCategory[3].equals("writer")) {
				st.setInt(1, Integer.parseInt(splitLineCategory[0]));
				st.setInt(2, Integer.parseInt(splitLineCategory[2]));

				st.addBatch();

			}
			if (cnt % step == 0) {
				st.executeBatch();
				con.commit();
			}
		}
		st.executeBatch();
		con.commit();
		sc.close();




//		String QUERY_Writer = "SELECT mid,pid FROM Writer";
//		PreparedStatement pstmt_Writer = con.prepareStatement(QUERY_Writer);
//		ResultSet rstWriter = pstmt_Writer.executeQuery();
//		System.out.println("MId\t\tPId\n");
//		while (rstWriter.next()) {
//			System.out.print(rstWriter.getInt(1));
//			System.out.print("\t\t" + rstWriter.getInt(2));
//			System.out.println();
//		}


		//--------KnownFor-------------////
		InputStream gzipStreamKnownFor = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
		sc = new Scanner(gzipStreamKnownFor, "UTF-8");

		st = con.prepareStatement("INSERT IGNORE INTO KnownFor(mid,pid) VALUES(?,?)");
		sc.nextLine();
//			line0Known = line0People.replaceAll(("titleType"), "");
		//System.out.println(line0People);
		cnt = 0;
		while (sc.hasNextLine()) {
			String lineKnown = sc.nextLine();
			lineKnown = lineKnown.substring(2);
			lineKnown = lineKnown.replaceAll(("^0+(?!$)"), "");
			// Split the line.
			String[] splitLineKnown = lineKnown.split("\t+");
			cnt++;


			String[] splitTitles = splitLineKnown[5].split(",");
			int i = 0;
			while (i < splitTitles.length) {
				splitTitles[i] = splitTitles[i].substring(2);
				splitTitles[i] = splitTitles[i].replaceAll(("^0+(?!$)"), "");

				st.setInt(2, Integer.parseInt(splitLineKnown[0]));
				st.setInt(1, Integer.parseInt(splitTitles[i]));
				i++;
				st.addBatch();

				if (/* Batch processing */ cnt % step==0) {
					st.executeBatch();
					//sc.close();
					con.commit();
				}
			}
		}
				st.executeBatch();
				con.commit();
				st.close();





//		String QUERY_KnownFor = "SELECT mid,pid FROM KnownFor";
//			PreparedStatement pstmt_KnownFor = con.prepareStatement(QUERY_KnownFor);
//			ResultSet rstKnownFor = pstmt_KnownFor.executeQuery();
//			System.out.println("MId\t\tPId\n");
//			while (rstKnownFor.next()) {
//				System.out.print(rstKnownFor.getInt(1));
//				System.out.print("\t\t" + rstKnownFor.getInt(2));
//				System.out.println();
//
//			}



			//--------People--------//
			InputStream gzipStreamPeople = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
			Scanner scPeople = new Scanner(gzipStreamPeople, "UTF-8");

			String line0People = scPeople.nextLine();
			cnt = 0;

		st = con.prepareStatement("INSERT IGNORE INTO Person(id,name, byear,dyear) VALUES(?,?,?,?)");
			while (scPeople.hasNextLine()) {
				String linePerson = scPeople.nextLine();
				linePerson = linePerson.substring(2);
				linePerson = linePerson.replaceAll(("^0+(?!$)"), "");
				// Split the line.
				String[] splitLinePerson = linePerson.split("\\t+");
				cnt++;


				st.setInt(1, Integer.parseInt(splitLinePerson[0]));

				// Set title and account for null; note the second argument of setNull.
				if (splitLinePerson[1].equals(NULL_STR))
					st.setNull(2, Types.VARCHAR);
				else
					st.setString(2, splitLinePerson[1]);

				if (splitLinePerson[2].equals(NULL_STR))
					st.setNull(3, Types.INTEGER);
				else
					st.setInt(3, Integer.parseInt(splitLinePerson[2]));

				if (splitLinePerson[3].equals(NULL_STR))
					st.setNull(4, Types.INTEGER);
				else
					st.setInt(4, Integer.parseInt(splitLinePerson[3]));


				st.addBatch();

				if (/* Batch processing */ cnt % step == 0) {
					st.executeBatch();
					con.commit();
				}
			}

				// Leftovers.

				st.executeBatch();
				st.close();
				con.commit();




//			String QUERY_PERSON = "SELECT id,name,byear,dyear FROM Person";
//			PreparedStatement pstmt_people = con.prepareStatement(QUERY_PERSON);
//			ResultSet rstPerson = pstmt_people.executeQuery();
//			System.out.println("Id\t\tName\t\tBYear\t\tDYear\n");
//			while (rstPerson.next()) {
//				System.out.print(rstPerson.getInt(1));
//				System.out.print("\t\t" + rstPerson.getString(2));
//				System.out.print("\t\t" + rstPerson.getString(3));
//				System.out.print("\t\t" + rstPerson.getString(4));
//				System.out.println();
//
//
//			}
			scPeople.close();

		}


	}






