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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GradeInitPoints {
	private static final String INITY_PLACEHOLDER = "@@InitYear@@", ENDY_PLACEHOLDER = "@@EndYear@@", ATTRIBS_PLACEHOLDER = "@@Attributes@@",
			GENRE_PLACEHOLDER = "@@Genre@@", TOTVOTES_PLACEHOLDER = "@@TotalVotes@@";
	private static final List<String> ALL_PLACEHOLDERS = Lists.newArrayList(INITY_PLACEHOLDER, ENDY_PLACEHOLDER, ATTRIBS_PLACEHOLDER, GENRE_PLACEHOLDER, TOTVOTES_PLACEHOLDER);
	
	private static final List<Document> settings = new ArrayList<>();
	
	static {
		settings.add(new Document().append(INITY_PLACEHOLDER, 1990).append(ENDY_PLACEHOLDER, 2010).append(GENRE_PLACEHOLDER, "%c%").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3").
				append("MaxMinutes", 5).append("ExpectedTotal", 54575).append("scaling", "MinMax").append("k", 75).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_106697230\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.1500000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"0.7000000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"0.9648484848484848484848484848484848\"}, \"dim_3\": {\"$numberDecimal\": \"0.06804261745741109516756718664759921\"}, \"dim_4\": {\"$numberDecimal\": \"0.1711026615969581749049429657794677\"}}}"),
							Document.parse("{\"_id\": \"p_283504464123\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.5500000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"0.6750000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"0.9769696969696969696969696969696969\"}, \"dim_3\": {\"$numberDecimal\": \"0.0002421906672352882332190825185723386\"}, \"dim_4\": {\"$numberDecimal\": \"0.1273764258555133079847908745247148\"}}}"),
							Document.parse("{\"_id\": \"p_806203665041\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.9500000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"0.6125000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"0.9842424242424242424242424242424241\"}, \"dim_3\": {\"$numberDecimal\": \"0.01813594996454215231362962404961426\"}, \"dim_4\": {\"$numberDecimal\": \"0.1140684410646387832699619771863118\"}}}"),
							Document.parse("{\"_id\": \"p_185284458\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.4500000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"0.4875000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"0.9672727272727272727272727272727272\"}, \"dim_3\": {\"$numberDecimal\": \"0.0006419267685082472402211468092594595\"}, \"dim_4\": {\"$numberDecimal\": \"0.1311787072243346007604562737642585\"}}}"),
							Document.parse("{\"_id\": \"p_106266171513\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.1500000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"0.5750000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"0.9721212121212121212121212121212120\"}, \"dim_3\": {\"$numberDecimal\": \"0.003273624018834171888143551835485307\"}, \"dim_4\": {\"$numberDecimal\": \"0.1349809885931558935361216730038023\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1990.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2010.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"2001.714631241410902427851580393953\"}, \"std\": {\"$numberDecimal\": \"5.833414523368167440790587434422239\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.100000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"9.100000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.246041227668346312414109024278516\"}, \"std\": {\"$numberDecimal\": \"1.092713066316963777123058333609298\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"354.0000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2004.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1959.636060467246907924874026568942\"}, \"std\": {\"$numberDecimal\": \"21.63850567320057457166741099689981\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2470130.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"46279.05031607879065506184150251947\"}, \"std\": {\"$numberDecimal\": \"128307.6852462296933133114667130538\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"25.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"551.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"106.3897755382501145213009619789281\"}, \"std\": {\"$numberDecimal\": \"21.50602668081612758110583282115596\"}}}")));

		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1990).append(ENDY_PLACEHOLDER, 2010).append(GENRE_PLACEHOLDER, "%c%").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3").
				append("MaxMinutes", 5).append("ExpectedTotal", 54575).append("scaling", "None").append("k", 75).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_3902051508692\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"2004.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"7.400000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1982.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"2561.000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"116.0000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_446719204583\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"2005.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"5.600000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1970.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"7178.000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"95.00000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_240772354\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"2001.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"7.700000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1970.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"547437.0000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"116.0000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_118954235389\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1997.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"7.400000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1936.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"44225.00000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"96.00000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_119558440830\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1997.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"6.900000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1951.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"56299.00000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"137.0000000000000000000000000000000\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1990.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2010.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"2001.714631241410902427851580393953\"}, \"std\": {\"$numberDecimal\": \"5.833414523368167440790587434422239\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.100000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"9.100000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.246041227668346312414109024278516\"}, \"std\": {\"$numberDecimal\": \"1.092713066316963777123058333609298\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"354.0000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2004.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1959.636060467246907924874026568942\"}, \"std\": {\"$numberDecimal\": \"21.63850567320057457166741099689981\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2470130.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"46279.05031607879065506184150251947\"}, \"std\": {\"$numberDecimal\": \"128307.6852462296933133114667130538\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"25.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"551.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"106.3897755382501145213009619789281\"}, \"std\": {\"$numberDecimal\": \"21.50602668081612758110583282115596\"}}}")));

		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1990).append(ENDY_PLACEHOLDER, 2010).append(GENRE_PLACEHOLDER, "%c%").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3").
				append("MaxMinutes", 5).append("ExpectedTotal", 54575).append("scaling", "Mean").append("k", 75).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_107247564627\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.4357315620705451213925790196976500\"}, \"dim_1\": {\"$numberDecimal\": \"0.03174484654145671094823637196518550\"}, \"dim_2\": {\"$numberDecimal\": \"-0.01190064270742236843931759185996485\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01708620745051343637981727220510531\"}, \"dim_4\": {\"$numberDecimal\": \"-0.01404900292442987551578129653788612\"}}}"),
							Document.parse("{\"_id\": \"p_1294226817023\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.4142684379294548786074209803023500\"}, \"dim_1\": {\"$numberDecimal\": \"-0.03075515345854328905176362803481450\"}, \"dim_2\": {\"$numberDecimal\": \"0.003250872444092783075833923291550303\"}, \"dim_3\": {\"$numberDecimal\": \"0.01590477843965269102786373595607217\"}, \"dim_4\": {\"$numberDecimal\": \"0.003061263235265941974713000040060646\"}}}"),
							Document.parse("{\"_id\": \"p_10438602473719\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.3142684379294548786074209803023500\"}, \"dim_1\": {\"$numberDecimal\": \"-0.01825515345854328905176362803481450\"}, \"dim_2\": {\"$numberDecimal\": \"0.01719026638348672246977331723094424\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01807886518528549567684063550447120\"}, \"dim_4\": {\"$numberDecimal\": \"0.001160122550855295586880300420288783\"}}}"),
							Document.parse("{\"_id\": \"p_102691941262\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.5357315620705451213925790196976500\"}, \"dim_1\": {\"$numberDecimal\": \"-0.1432551534585432890517636280348145\"}, \"dim_2\": {\"$numberDecimal\": \"-0.002203673010452671469620622162995151\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01812584531471575225719751438767252\"}, \"dim_4\": {\"$numberDecimal\": \"-0.01785128429325116829144669577742985\"}}}"),
							Document.parse("{\"_id\": \"p_1228931897201\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.3142684379294548786074209803023500\"}, \"dim_1\": {\"$numberDecimal\": \"-0.2432551534585432890517636280348145\"}, \"dim_2\": {\"$numberDecimal\": \"0.008705417898638237621288468746095757\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01703841731885162365152320575495224\"}, \"dim_4\": {\"$numberDecimal\": \"0.08290917198051309026368638407047889\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1990.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2010.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"2001.714631241410902427851580393953\"}, \"std\": {\"$numberDecimal\": \"5.833414523368167440790587434422239\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.100000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"9.100000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.246041227668346312414109024278516\"}, \"std\": {\"$numberDecimal\": \"1.092713066316963777123058333609298\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"354.0000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2004.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1959.636060467246907924874026568942\"}, \"std\": {\"$numberDecimal\": \"21.63850567320057457166741099689981\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2470130.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"46279.05031607879065506184150251947\"}, \"std\": {\"$numberDecimal\": \"128307.6852462296933133114667130538\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"25.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"551.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"106.3897755382501145213009619789281\"}, \"std\": {\"$numberDecimal\": \"21.50602668081612758110583282115596\"}}}")));

		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1990).append(ENDY_PLACEHOLDER, 2010).append(GENRE_PLACEHOLDER, "%c%").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3").
				append("MaxMinutes", 5).append("ExpectedTotal", 54575).append("scaling", "ZScore").append("k", 75).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_2556535220\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.1225065077319895307600621024233709\"}, \"dim_1\": {\"$numberDecimal\": \"0.4154418816110080810686997165510830\"}, \"dim_2\": {\"$numberDecimal\": \"0.7562416638141484118430325577722662\"}, \"dim_3\": {\"$numberDecimal\": \"-0.3231532876343989508317873639276360\"}, \"dim_4\": {\"$numberDecimal\": \"-0.2041183898542214527527973315527650\"}}}"),
							Document.parse("{\"_id\": \"p_243862304644\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.2939326932009090108049427270165362\"}, \"dim_1\": {\"$numberDecimal\": \"0.7815032131078640157172020177728948\"}, \"dim_2\": {\"$numberDecimal\": \"-0.07560875468739872863993842646801468\"}, \"dim_3\": {\"$numberDecimal\": \"-0.2574128763424895169634872536839112\"}, \"dim_4\": {\"$numberDecimal\": \"0.2608675486650602572961196691606781\"}}}"),
							Document.parse("{\"_id\": \"p_16800193759338\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1.420329161488285789643863518915117\"}, \"dim_1\": {\"$numberDecimal\": \"1.147564544604719950365704318994707\"}, \"dim_2\": {\"$numberDecimal\": \"1.218380785203896823222460882350200\"}, \"dim_3\": {\"$numberDecimal\": \"-0.3336281083547695826182716968461382\"}, \"dim_4\": {\"$numberDecimal\": \"-0.5761071406696468207919309321235194\"}}}"),
							Document.parse("{\"_id\": \"p_1349853490513\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1.248902976019366309598982894321952\"}, \"dim_1\": {\"$numberDecimal\": \"0.7815032131078640157172020177728948\"}, \"dim_2\": {\"$numberDecimal\": \"0.2016747181464503181877185682787456\"}, \"dim_3\": {\"$numberDecimal\": \"-0.3364260701394519165552864851777917\"}, \"dim_4\": {\"$numberDecimal\": \"-0.2971155775580777947625807316954536\"}}}"),
							Document.parse("{\"_id\": \"p_109683504899\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-1.322489806014425891074226474575528\"}, \"dim_1\": {\"$numberDecimal\": \"0.5069572144852220647308252918565359\"}, \"dim_2\": {\"$numberDecimal\": \"-0.07560875468739872863993842646801468\"}, \"dim_3\": {\"$numberDecimal\": \"-0.3407516099458549676278302387267267\"}, \"dim_4\": {\"$numberDecimal\": \"0.9118478625920546513646034701594985\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1990.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2010.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"2001.714631241410902427851580393953\"}, \"std\": {\"$numberDecimal\": \"5.833414523368167440790587434422239\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.100000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"9.100000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.246041227668346312414109024278516\"}, \"std\": {\"$numberDecimal\": \"1.092713066316963777123058333609298\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"354.0000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2004.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1959.636060467246907924874026568942\"}, \"std\": {\"$numberDecimal\": \"21.63850567320057457166741099689981\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2470130.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"46279.05031607879065506184150251947\"}, \"std\": {\"$numberDecimal\": \"128307.6852462296933133114667130538\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"25.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"551.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"106.3897755382501145213009619789281\"}, \"std\": {\"$numberDecimal\": \"21.50602668081612758110583282115596\"}}}")));
		
		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1930).append(ENDY_PLACEHOLDER, 1980).append(GENRE_PLACEHOLDER, "Action").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, byear AS dim_0, rating AS dim_1, year AS dim_2, totalvotes AS dim_4, runtime AS dim_3").
				append("MaxMinutes", 2).append("ExpectedTotal", 6076).append("scaling", "MinMax").append("k", 50).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_598852145\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.8942652329749103942652329749103942\"}, \"dim_1\": {\"$numberDecimal\": \"0.7714285714285714285714285714285717\"}, \"dim_2\": {\"$numberDecimal\": \"0.7000000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"0.3080568720379146919431279620853080\"}, \"dim_4\": {\"$numberDecimal\": \"0.009746319262609329502344691684432992\"}}}"),
							Document.parse("{\"_id\": \"p_64137247614\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.9014336917562724014336917562724014\"}, \"dim_1\": {\"$numberDecimal\": \"0.6285714285714285714285714285714288\"}, \"dim_2\": {\"$numberDecimal\": \"0.7800000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"0.2606635071090047393364928909952606\"}, \"dim_4\": {\"$numberDecimal\": \"0.001593757166695108231091700494111052\"}}}"),
							Document.parse("{\"_id\": \"p_31747459678\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.8781362007168458781362007168458781\"}, \"dim_1\": {\"$numberDecimal\": \"0.6714285714285714285714285714285716\"}, \"dim_2\": {\"$numberDecimal\": \"0.1800000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"0.1563981042654028436018957345971564\"}, \"dim_4\": {\"$numberDecimal\": \"0.0008949440679261775386798841437377466\"}}}"),
							Document.parse("{\"_id\": \"p_617092759\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.9318996415770609318996415770609318\"}, \"dim_1\": {\"$numberDecimal\": \"0.7714285714285714285714285714285717\"}, \"dim_2\": {\"$numberDecimal\": \"0.7400000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"0.2796208530805687203791469194312796\"}, \"dim_4\": {\"$numberDecimal\": \"0.002801429748435006134884054938292101\"}}}"),
							Document.parse("{\"_id\": \"p_68767159494\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.9193548387096774193548387096774193\"}, \"dim_1\": {\"$numberDecimal\": \"0.8000000000000000000000000000000002\"}, \"dim_2\": {\"$numberDecimal\": \"0.8400000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"0.2559241706161137440758293838862559\"}, \"dim_4\": {\"$numberDecimal\": \"0.02187014739937284420019875634433494\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1414.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1972.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1922.740454246214614878209348255431\"}, \"std\": {\"$numberDecimal\": \"19.46452224244854840114575532045623\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.700000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.700000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.547086899275839368005266622778144\"}, \"std\": {\"$numberDecimal\": \"0.913260733615391586362344268275052\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1930.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1980.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1965.461981566820276497695852534562\"}, \"std\": {\"$numberDecimal\": \"12.42613606192803210572314488603863\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"52.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"263.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.9282422646477946017116524028966\"}, \"std\": {\"$numberDecimal\": \"25.38615110276222204390825485160993\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1296054.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"15088.62936142198815009874917709019\"}, \"std\": {\"$numberDecimal\": \"71663.41489029254375695037242214782\"}}}")));
		

		settings.add(new Document().append(INITY_PLACEHOLDER, 1930).append(ENDY_PLACEHOLDER, 1980).append(GENRE_PLACEHOLDER, "Action").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, byear AS dim_0, rating AS dim_1, year AS dim_2, totalvotes AS dim_4, runtime AS dim_3").
				append("MaxMinutes", 2).append("ExpectedTotal", 6076).append("scaling", "None").append("k", 50).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_344652063\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1911.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"6.100000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1942.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"86.00000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"1428.000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_73880470709\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1926.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"6.100000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1975.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"109.0000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"1303.000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_36644581197\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1919.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"6.400000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1944.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"74.00000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"1239.000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_4076149608\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1896.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"7.000000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1948.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"111.0000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"1924.000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_4349642046\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1901.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"5.100000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1966.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"86.00000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"1150.000000000000000000000000000000\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1414.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1972.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1922.740454246214614878209348255431\"}, \"std\": {\"$numberDecimal\": \"19.46452224244854840114575532045623\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.700000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.700000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.547086899275839368005266622778144\"}, \"std\": {\"$numberDecimal\": \"0.913260733615391586362344268275052\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1930.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1980.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1965.461981566820276497695852534562\"}, \"std\": {\"$numberDecimal\": \"12.42613606192803210572314488603863\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"52.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"263.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.9282422646477946017116524028966\"}, \"std\": {\"$numberDecimal\": \"25.38615110276222204390825485160993\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1296054.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"15088.62936142198815009874917709019\"}, \"std\": {\"$numberDecimal\": \"71663.41489029254375695037242214782\"}}}")));
		
		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1930).append(ENDY_PLACEHOLDER, 1980).append(GENRE_PLACEHOLDER, "Action").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, byear AS dim_0, rating AS dim_1, year AS dim_2, totalvotes AS dim_4, runtime AS dim_3").
				append("MaxMinutes", 2).append("ExpectedTotal", 6076).append("scaling", "Mean").append("k", 50).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_3519568\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.04433773162404052845557230870148924\"}, \"dim_1\": {\"$numberDecimal\": \"-0.006726699896548481143609517539734859\"}, \"dim_2\": {\"$numberDecimal\": \"-0.4692396313364055299539170506912400\"}, \"dim_3\": {\"$numberDecimal\": \"-0.06127129035378101706972347110377535\"}, \"dim_4\": {\"$numberDecimal\": \"-0.01068807945421692251212788138947996\"}}}"),
							Document.parse("{\"_id\": \"p_59390947343\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.007633594540834023515753856173062724\"}, \"dim_1\": {\"$numberDecimal\": \"-0.09244098561083419542932380325402060\"}, \"dim_2\": {\"$numberDecimal\": \"-0.009239631336405529953917050691240000\"}, \"dim_3\": {\"$numberDecimal\": \"0.0003400840538019213189021213132862559\"}, \"dim_4\": {\"$numberDecimal\": \"-0.01083556376566981285715623158055322\"}}}"),
							Document.parse("{\"_id\": \"p_70297396449\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.01300993862685552889209794219456810\"}, \"dim_1\": {\"$numberDecimal\": \"-0.09244098561083419542932380325402060\"}, \"dim_2\": {\"$numberDecimal\": \"0.1707603686635944700460829493087600\"}, \"dim_3\": {\"$numberDecimal\": \"-0.07548929983245400285171399243078957\"}, \"dim_4\": {\"$numberDecimal\": \"-0.008444155846457240089864082147286783\"}}}"),
							Document.parse("{\"_id\": \"p_698182017\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.02283235527995450695019596461546774\"}, \"dim_1\": {\"$numberDecimal\": \"0.06470187153202294742781905388883659\"}, \"dim_2\": {\"$numberDecimal\": \"0.1507603686635944700460829493087600\"}, \"dim_3\": {\"$numberDecimal\": \"0.01929743002536590236155614974930521\"}, \"dim_4\": {\"$numberDecimal\": \"-0.01019389118547425329318471844556956\"}}}"),
							Document.parse("{\"_id\": \"p_25004869371\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.05150619040540253562403109006349641\"}, \"dim_1\": {\"$numberDecimal\": \"0.1218447286748800902849619110316938\"}, \"dim_2\": {\"$numberDecimal\": \"-0.6292396313364055299539170506912400\"}, \"dim_3\": {\"$numberDecimal\": \"0.03825477599692988340421017818532417\"}, \"dim_4\": {\"$numberDecimal\": \"-0.009529825699351291530229843239689951\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1414.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1972.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1922.740454246214614878209348255431\"}, \"std\": {\"$numberDecimal\": \"19.46452224244854840114575532045623\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.700000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.700000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.547086899275839368005266622778144\"}, \"std\": {\"$numberDecimal\": \"0.913260733615391586362344268275052\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1930.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1980.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1965.461981566820276497695852534562\"}, \"std\": {\"$numberDecimal\": \"12.42613606192803210572314488603863\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"52.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"263.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.9282422646477946017116524028966\"}, \"std\": {\"$numberDecimal\": \"25.38615110276222204390825485160993\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1296054.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"15088.62936142198815009874917709019\"}, \"std\": {\"$numberDecimal\": \"71663.41489029254375695037242214782\"}}}")));

		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1930).append(ENDY_PLACEHOLDER, 1980).append(GENRE_PLACEHOLDER, "Action").append(TOTVOTES_PLACEHOLDER, 1000).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, byear AS dim_0, rating AS dim_1, year AS dim_2, totalvotes AS dim_4, runtime AS dim_3").
				append("MaxMinutes", 2).append("ExpectedTotal", 6076).append("scaling", "ZScore").append("k", 50).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_35244494435\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-1.219678240775981854864424881597294\"}, \"dim_1\": {\"$numberDecimal\": \"0.1674364122924812892609539103588217\"}, \"dim_2\": {\"$numberDecimal\": \"-1.888115617750601788224376101879652\"}, \"dim_3\": {\"$numberDecimal\": \"0.7118746619839448412664871213571197\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1661884153811829433279398229395090\"}}}"),
							Document.parse("{\"_id\": \"p_6816812524\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.7325916134066694204198952918707052\"}, \"dim_1\": {\"$numberDecimal\": \"0.4959296770936166827222248045597271\"}, \"dim_2\": {\"$numberDecimal\": \"0.5261505588379408371310255034700450\"}, \"dim_3\": {\"$numberDecimal\": \"-0.1153480199812242415195561078210781\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1280518012638697937308518100958959\"}}}"),
							Document.parse("{\"_id\": \"p_4451758982\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.2188363886217611900819162988528106\"}, \"dim_1\": {\"$numberDecimal\": \"0.8244229418947520761834956987606325\"}, \"dim_2\": {\"$numberDecimal\": \"-1.083360225554420913105908900096419\"}, \"dim_3\": {\"$numberDecimal\": \"0.002826648870942770307021496347235869\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1237818400783967517342513959246023\"}}}"),
							Document.parse("{\"_id\": \"p_23551919321\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.9628006283835277396954353850883470\"}, \"dim_1\": {\"$numberDecimal\": \"0.4959296770936166827222248045597271\"}, \"dim_2\": {\"$numberDecimal\": \"-2.692871009946782663342843303662884\"}, \"dim_3\": {\"$numberDecimal\": \"-0.1941311325493355827372745105999541\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1075113343847315002047217131673199\"}}}"),
							Document.parse("{\"_id\": \"p_340936807\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.1407922687276745711546689962597157\"}, \"dim_1\": {\"$numberDecimal\": \"0.7149251869610402783630720673603307\"}, \"dim_2\": {\"$numberDecimal\": \"-1.968591156970219875736222822057975\"}, \"dim_3\": {\"$numberDecimal\": \"-0.5486551391058366182170073231048960\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1960223271934096223760825860056065\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1414.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1972.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1922.740454246214614878209348255431\"}, \"std\": {\"$numberDecimal\": \"19.46452224244854840114575532045623\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.700000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.700000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.547086899275839368005266622778144\"}, \"std\": {\"$numberDecimal\": \"0.913260733615391586362344268275052\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1930.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1980.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1965.461981566820276497695852534562\"}, \"std\": {\"$numberDecimal\": \"12.42613606192803210572314488603863\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"52.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"263.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.9282422646477946017116524028966\"}, \"std\": {\"$numberDecimal\": \"25.38615110276222204390825485160993\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"1001.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"1296054.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"15088.62936142198815009874917709019\"}, \"std\": {\"$numberDecimal\": \"71663.41489029254375695037242214782\"}}}")));

		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1950).append(ENDY_PLACEHOLDER, 2020).append(GENRE_PLACEHOLDER, "Sci-Fi").append(TOTVOTES_PLACEHOLDER, 3500).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, byear AS dim_2, totalvotes AS dim_3, runtime AS dim_4, rating AS dim_1").
				append("MaxMinutes", 1).append("ExpectedTotal", 6700).append("scaling", "MinMax").append("k", 100).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_96256202966\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.5428571428571428571428571428571430\"}, \"dim_1\": {\"$numberDecimal\": \"0.7945205479452054794520547945205475\"}, \"dim_2\": {\"$numberDecimal\": \"0.8808988764044943820224719101123598\"}, \"dim_3\": {\"$numberDecimal\": \"0.05403916901495064081926473088484787\"}, \"dim_4\": {\"$numberDecimal\": \"0.3380281690140845070422535211267605\"}}}"),
							Document.parse("{\"_id\": \"p_1677720672015\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.9714285714285714285714285714285717\"}, \"dim_1\": {\"$numberDecimal\": \"0.8082191780821917808219178082191777\"}, \"dim_2\": {\"$numberDecimal\": \"0.9078651685393258426966292134831463\"}, \"dim_3\": {\"$numberDecimal\": \"0.1832045846702575214420332582123621\"}, \"dim_4\": {\"$numberDecimal\": \"0.6619718309859154929577464788732393\"}}}"),
							Document.parse("{\"_id\": \"p_59941662640887\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.9714285714285714285714285714285717\"}, \"dim_1\": {\"$numberDecimal\": \"0.5479452054794520547945205479452052\"}, \"dim_2\": {\"$numberDecimal\": \"0.9707865168539325842696629213483149\"}, \"dim_3\": {\"$numberDecimal\": \"0.00005878246148392254461136653849075578\"}, \"dim_4\": {\"$numberDecimal\": \"0.3098591549295774647887323943661971\"}}}"),
							Document.parse("{\"_id\": \"p_113481519434\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.6428571428571428571428571428571430\"}, \"dim_1\": {\"$numberDecimal\": \"0.5753424657534246575342465753424655\"}, \"dim_2\": {\"$numberDecimal\": \"0.8741573033707865168539325842696632\"}, \"dim_3\": {\"$numberDecimal\": \"0.03013957669392967085438951248539482\"}, \"dim_4\": {\"$numberDecimal\": \"0.4014084507042253521126760563380281\"}}}"),
							Document.parse("{\"_id\": \"p_884328390229\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.8142857142857142857142857142857145\"}, \"dim_1\": {\"$numberDecimal\": \"0.7671232876712328767123287671232873\"}, \"dim_2\": {\"$numberDecimal\": \"0.9101123595505617977528089887640452\"}, \"dim_3\": {\"$numberDecimal\": \"0.1318052003487156484338543569560452\"}, \"dim_4\": {\"$numberDecimal\": \"0.5633802816901408450704225352112675\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1950.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2020.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1997.281194029850746268656716417910\"}, \"std\": {\"$numberDecimal\": \"18.34182167765485965750368277009502\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.500000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.800000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.109194029850746268656716417910448\"}, \"std\": {\"$numberDecimal\": \"1.136955455864948886696041345562377\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1564.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2009.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1953.994179104477611940298507462687\"}, \"std\": {\"$numberDecimal\": \"25.08840718484135249944978363365972\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"3505.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2215049.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"131973.1968656716417910447761194030\"}, \"std\": {\"$numberDecimal\": \"227036.436511229431699434166117932\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"46.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"188.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.2714925373134328358208955223881\"}, \"std\": {\"$numberDecimal\": \"19.23045297576033134394973437687994\"}}}")));
		
		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1950).append(ENDY_PLACEHOLDER, 2020).append(GENRE_PLACEHOLDER, "Sci-Fi").append(TOTVOTES_PLACEHOLDER, 3500).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, byear AS dim_2, totalvotes AS dim_3, runtime AS dim_4, rating AS dim_1").
				append("MaxMinutes", 1).append("ExpectedTotal", 6700).append("scaling", "None").append("k", 100).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_6451926755\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"1969.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"6.300000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1929.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"3599.000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"101.0000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_1223934775831\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"2009.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"6.700000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1955.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"5641.000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"103.0000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_10251001125275\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"2019.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"5.700000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1970.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"107020.0000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"117.0000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_1707391353\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"2011.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"4.600000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1955.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"5336.000000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"82.00000000000000000000000000000000\"}}}"),
							Document.parse("{\"_id\": \"p_61334664996\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"2018.000000000000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"5.200000000000000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1965.000000000000000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"61234.00000000000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"97.00000000000000000000000000000000\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1950.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2020.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1997.281194029850746268656716417910\"}, \"std\": {\"$numberDecimal\": \"18.34182167765485965750368277009502\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.500000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.800000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.109194029850746268656716417910448\"}, \"std\": {\"$numberDecimal\": \"1.136955455864948886696041345562377\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1564.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2009.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1953.994179104477611940298507462687\"}, \"std\": {\"$numberDecimal\": \"25.08840718484135249944978363365972\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"3505.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2215049.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"131973.1968656716417910447761194030\"}, \"std\": {\"$numberDecimal\": \"227036.436511229431699434166117932\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"46.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"188.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.2714925373134328358208955223881\"}, \"std\": {\"$numberDecimal\": \"19.23045297576033134394973437687994\"}}}")));
		
		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1950).append(ENDY_PLACEHOLDER, 2020).append(GENRE_PLACEHOLDER, "Sci-Fi").append(TOTVOTES_PLACEHOLDER, 3500).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, byear AS dim_2, totalvotes AS dim_3, runtime AS dim_4, rating AS dim_1").
				append("MaxMinutes", 1).append("ExpectedTotal", 6700).append("scaling", "Mean").append("k", 100).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_88763417\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.1754456289978678038379530916844286\"}, \"dim_1\": {\"$numberDecimal\": \"0.3275076671437333878552443263136371\"}, \"dim_2\": {\"$numberDecimal\": \"0.02248499077645480462854267985913034\"}, \"dim_3\": {\"$numberDecimal\": \"0.4530883415090671305698440654495669\"}, \"dim_4\": {\"$numberDecimal\": \"0.08259512297666596594492327096909787\"}}}"),
							Document.parse("{\"_id\": \"p_1104001310\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.1816972281449893390191897654584286\"}, \"dim_1\": {\"$numberDecimal\": \"0.09463095481496626456757309343692488\"}, \"dim_2\": {\"$numberDecimal\": \"-0.008975683380848566157974174073453935\"}, \"dim_3\": {\"$numberDecimal\": \"0.08791948210586285337707738298699778\"}, \"dim_4\": {\"$numberDecimal\": \"0.1459754046668068110153458061803655\"}}}"),
							Document.parse("{\"_id\": \"p_1069502340\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.07544562899786780383795309168442859\"}, \"dim_1\": {\"$numberDecimal\": \"-0.02865671641791044776119402985074629\"}, \"dim_2\": {\"$numberDecimal\": \"-0.01571725641455643132651349991615057\"}, \"dim_3\": {\"$numberDecimal\": \"-0.04678143273010694871593998406516127\"}, \"dim_4\": {\"$numberDecimal\": \"-0.06529220096699600588606264452385985\"}}}"),
							Document.parse("{\"_id\": \"p_101988157695\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.08973134328358208955223880597014288\"}, \"dim_1\": {\"$numberDecimal\": \"-0.1519443876507871600899611531384175\"}, \"dim_2\": {\"$numberDecimal\": \"0.01349622673151098440382357873553484\"}, \"dim_3\": {\"$numberDecimal\": \"-0.05733243239369039991564480567395585\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1145879756148833298297246163548458\"}}}"),
							Document.parse("{\"_id\": \"p_59126511614\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"-0.4611599147121535181236673773987144\"}, \"dim_1\": {\"$numberDecimal\": \"-0.05605397669188305050092005724800655\"}, \"dim_2\": {\"$numberDecimal\": \"-0.03369478450444407177595170216334158\"}, \"dim_3\": {\"$numberDecimal\": \"-0.05770050103713588415651905461496719\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1568414967416438932100063064956908\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1950.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2020.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1997.281194029850746268656716417910\"}, \"std\": {\"$numberDecimal\": \"18.34182167765485965750368277009502\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.500000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.800000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.109194029850746268656716417910448\"}, \"std\": {\"$numberDecimal\": \"1.136955455864948886696041345562377\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1564.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2009.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1953.994179104477611940298507462687\"}, \"std\": {\"$numberDecimal\": \"25.08840718484135249944978363365972\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"3505.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2215049.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"131973.1968656716417910447761194030\"}, \"std\": {\"$numberDecimal\": \"227036.436511229431699434166117932\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"46.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"188.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.2714925373134328358208955223881\"}, \"std\": {\"$numberDecimal\": \"19.23045297576033134394973437687994\"}}}")));
		
		
		settings.add(new Document().append(INITY_PLACEHOLDER, 1950).append(ENDY_PLACEHOLDER, 2020).append(GENRE_PLACEHOLDER, "Sci-Fi").append(TOTVOTES_PLACEHOLDER, 3500).
				append(ATTRIBS_PLACEHOLDER, "CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, byear AS dim_2, totalvotes AS dim_3, runtime AS dim_4, rating AS dim_1").
				append("MaxMinutes", 1).append("ExpectedTotal", 6700).append("scaling", "ZScore").append("k", 100).
					append("ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_16317071036050\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.7479522051434155378771618731707931\"}, \"dim_1\": {\"$numberDecimal\": \"0.3437302386239438414249384341273107\"}, \"dim_2\": {\"$numberDecimal\": \"1.076426283125680240311594785341588\"}, \"dim_3\": {\"$numberDecimal\": \"-0.5340428995839821663409462010980856\"}, \"dim_4\": {\"$numberDecimal\": \"-0.7421298164584273540799363177660353\"}}}"),
							Document.parse("{\"_id\": \"p_892899104114\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.5843915701790714377060169838073259\"}, \"dim_1\": {\"$numberDecimal\": \"-0.2719491148538657431621992285892674\"}, \"dim_2\": {\"$numberDecimal\": \"0.3988224848952546307332802923333982\"}, \"dim_3\": {\"$numberDecimal\": \"-0.5061266756624244622313384133214898\"}, \"dim_4\": {\"$numberDecimal\": \"-0.7421298164584273540799363177660353\"}}}"),
							Document.parse("{\"_id\": \"p_2369396550452\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.8569926284529782713245917994131045\"}, \"dim_1\": {\"$numberDecimal\": \"-0.5358116949157841365566867983249437\"}, \"dim_2\": {\"$numberDecimal\": \"0.7575539074878328946276820827494985\"}, \"dim_3\": {\"$numberDecimal\": \"-0.5499390264588483313979948767190597\"}, \"dim_4\": {\"$numberDecimal\": \"-0.6381281061232123178980985380563035\"}}}"),
							Document.parse("{\"_id\": \"p_3717241134029\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.4208309352147273375348720944438587\"}, \"dim_1\": {\"$numberDecimal\": \"0.5196386253318894370212634806177616\"}, \"dim_2\": {\"$numberDecimal\": \"0.7176948605331019764171929949254874\"}, \"dim_3\": {\"$numberDecimal\": \"0.2707618392842765422276140675260199\"}, \"dim_4\": {\"$numberDecimal\": \"0.2458864317261154896475225894764168\"}}}"),
							Document.parse("{\"_id\": \"p_381940710770\", \"point\": {\"dim_0\": {\"$numberDecimal\": \"0.6389117818338528044297319469284816\"}, \"dim_1\": {\"$numberDecimal\": \"-0.008086534791947349767711658853591086\"}, \"dim_2\": {\"$numberDecimal\": \"0.7575539074878328946276820827494985\"}, \"dim_3\": {\"$numberDecimal\": \"-0.4958155554036099173716678814210463\"}, \"dim_4\": {\"$numberDecimal\": \"0.4018889972289380439202792590410145\"}}}")
						)).append("Limits", 
								Document.parse("{\"_id\": \"limits\", \"dim_0\": {\"min\": {\"$numberDecimal\": \"1950.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2020.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1997.281194029850746268656716417910\"}, \"std\": {\"$numberDecimal\": \"18.34182167765485965750368277009502\"}}, \"dim_1\": {\"min\": {\"$numberDecimal\": \"1.500000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"8.800000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"6.109194029850746268656716417910448\"}, \"std\": {\"$numberDecimal\": \"1.136955455864948886696041345562377\"}}, \"dim_2\": {\"min\": {\"$numberDecimal\": \"1564.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2009.000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"1953.994179104477611940298507462687\"}, \"std\": {\"$numberDecimal\": \"25.08840718484135249944978363365972\"}}, \"dim_3\": {\"min\": {\"$numberDecimal\": \"3505.000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"2215049.000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"131973.1968656716417910447761194030\"}, \"std\": {\"$numberDecimal\": \"227036.436511229431699434166117932\"}}, \"dim_4\": {\"min\": {\"$numberDecimal\": \"46.00000000000000000000000000000000\"}, \"max\": {\"$numberDecimal\": \"188.0000000000000000000000000000000\"}, \"mean\": {\"$numberDecimal\": \"104.2714925373134328358208955223881\"}, \"std\": {\"$numberDecimal\": \"19.23045297576033134394973437687994\"}}}")));
	}


	public static void main(String[] args) {
		final String jdbcUser = args[0];
		final String jdbcPwd = args[1];
		final String jdbcSchema = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];
		final String folderToAssignment = args[5];
		final boolean windows = Boolean.valueOf(args[6]);
		
		// Get JDBC URL.
		final String jdbcURL = "jdbc:mysql://localhost:3306/" + jdbcSchema+"?useCursorFetch=true";
		
		// Name of the folder containing the Gradle project.
		final String folderToSearch = "InitPointsAndCentroids";
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
						String sql = substitutePlaceholders(setting, ""
							+ "SELECT @@Attributes@@ FROM Actor JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE "
							+ "	byear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN @@InitYear@@ AND @@EndYear@@ AND "
								+ "totalvotes > @@TotalVotes@@ AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '@@Genre@@') UNION "
							+ "SELECT @@Attributes@@ FROM Director JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE "
							+ "	byear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN @@InitYear@@ AND @@EndYear@@ AND "
								+ "totalvotes > @@TotalVotes@@ AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '@@Genre@@') UNION "
							+ "SELECT @@Attributes@@ FROM Producer JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE "
							+ "	byear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN @@InitYear@@ AND @@EndYear@@ AND "
								+ "totalvotes > @@TotalVotes@@ AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '@@Genre@@') UNION "
							+ "SELECT @@Attributes@@ FROM Writer JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE "
							+ "	byear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN @@InitYear@@ AND @@EndYear@@ AND "
								+ "totalvotes > @@TotalVotes@@ AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '@@Genre@@')");
						String mongoCol = "Points_"+current;
						
						// Destroy the existing collection!
						client = getClient(mongoDBURL);
						db = client.getDatabase(mongoDBName);
						db.getCollection(mongoCol).drop();
						client.close();
						
						// Let's run the program!
						ProcessBuilder builder = new ProcessBuilder(gradleProject.getAbsolutePath() + 
								"/app/build/install/app/bin/app" + (windows?".bat":""), jdbcURL, jdbcUser, jdbcPwd, (windows?"\"":"")+sql+(windows?"\"":""), 
								mongoDBURL, mongoDBName, mongoCol, setting.get("scaling").toString(), setting.get("k").toString()).redirectErrorStream(true);
						builder.environment().put("JAVA_OPTS", "-Xmx" + maxMem + "m");
				
						Process process = null;
						try {
							long before = System.nanoTime();
							// Start the process.
							process = builder.start();
							// Change false/true to print to console the output of the program.
							StreamGobbler gobbler = new GradeInitPoints().new StreamGobbler(process.getInputStream(), true);
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
							
							MongoCollection<Document> collection = db.getCollection(mongoCol);
							
							if (collection == null) {
								System.out.println("The expected collection did not exist.");
								System.out.println("\tPenalty: -15");
							} else {
								long cnt = 0;
								if ((cnt = collection.countDocuments(Document.parse("{_id : /p\\_.*/}"))) != setting.getInteger("ExpectedTotal")) {
									System.out.println("The total number of points were not as expected: "+cnt + " instead of " + setting.getInteger("ExpectedTotal"));
									System.out.println("\tPenalty: -10");
								}
								
								if ((cnt = collection.countDocuments(Document.parse("{_id : /c\\_.*/}"))) != setting.getInteger("k")) {
									System.out.println("The total number of centroids were not as expected: "+cnt + " instead of " + setting.getInteger("k"));
									System.out.println("\tPenalty: -10");
								}
								
								for (Document doc : setting.getList("ExpectedSamples", Document.class)) {
									Document other = collection.find(new Document().append("_id", doc.get("_id"))).first();
									if (other == null || !Maps.difference(doc, other).areEqual()) {
										System.out.println("Expected example is: " + doc);
										System.out.println("Example retrieved: " + other);
										System.out.println("\tPenalty: -5");
									}
								}
								
								Document other = collection.find(new Document().append("_id", "limits")).first();
								if (other == null || !Maps.difference(setting.get("Limits", Document.class), other).areEqual()) {
									System.out.println("Limits is: " + setting.get("Limits", Document.class));
									System.out.println("Limits retrieved: " + other);
									System.out.println("\tPenalty: -5");
								}
								
//								collection.drop();
							}




							
							client.close();
						} catch (Exception oops) {
							System.out.println("A major problem happened!");
							oops.printStackTrace(System.out);
						} finally {
							destroyProcess(process);
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
	
	private static String substitutePlaceholders(Document setting, String query) {
		String thisQuery = query;
		for (String placeholder : ALL_PLACEHOLDERS)
			if (setting.containsKey(placeholder))
				thisQuery = thisQuery.replace(placeholder, setting.get(placeholder).toString());
		return thisQuery;
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
			StreamGobbler gobbler = new GradeInitPoints().new StreamGobbler(process.getInputStream(), true);
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
