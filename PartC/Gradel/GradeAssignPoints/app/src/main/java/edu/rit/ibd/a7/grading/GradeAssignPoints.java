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

public class GradeAssignPoints {
	
	private static final List<Document> settings = new ArrayList<>();
	
	static {
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Manhattan").append("Mean", "Arithmetic").
					append("Epoch0_CentroidReinitializations", 8).
					append("Epoch0_ExpectedSamples", Lists.newArrayList(
							Document.parse("{\"_id\": \"p_1155696370677\", \"label\": \"c_33\"}"),
							Document.parse("{\"_id\": \"p_108122510\", \"label\": \"c_64\"}"),
							Document.parse("{\"_id\": \"p_109417438\", \"label\": \"c_36\"}"),
							Document.parse("{\"_id\": \"p_174741286294\", \"label\": \"c_61\"}"),
							Document.parse("{\"_id\": \"p_120851185\", \"label\": \"c_17\"}")
						)).
					append("Epoch0_ExpectedCentroids", Lists.newArrayList(
							Document.parse("{\"_id\": \"c_51\", \"sse\": {\"$numberDecimal\": \"69.30357982578257804712240311890883\"}, \"total_points\": 426}"),
							Document.parse("{\"_id\": \"c_29\", \"sse\": {\"$numberDecimal\": \"372.8042644348448267634717203249576\"}, \"total_points\": 2029}"),
							Document.parse("{\"_id\": \"c_20\", \"sse\": {\"$numberDecimal\": \"7.537380546163851048749978449096116\"}, \"total_points\": 63}"),
							Document.parse("{\"_id\": \"c_73\", \"sse\": {\"$numberDecimal\": \"257.9774424153842119633282623867666\"}, \"total_points\": 1549}"),
							Document.parse("{\"_id\": \"c_43\", \"sse\": {\"$numberDecimal\": \"14.39498807400300467011687758549162\"}, \"total_points\": 157}")
							)).
					append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
							Document.parse("{\"_id\": \"new_c_73\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.3202388637830858618463524854744997\"}, \"dim_1\": {\"$numberDecimal\": \"0.6871772111039380245319561007101356\"}, \"dim_2\": {\"$numberDecimal\": \"0.9771508500107596298687325156014590\"}, \"dim_3\": {\"$numberDecimal\": \"0.009937087505791636880749793683241414\"}, \"dim_4\": {\"$numberDecimal\": \"0.1660411353332335101512812141771827\"}}}"),
							Document.parse("{\"_id\": \"new_c_20\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.2071428571428571428571428571428571\"}, \"dim_1\": {\"$numberDecimal\": \"0.4240079365079365079365079365079365\"}, \"dim_2\": {\"$numberDecimal\": \"0.9551996151996151996151996151996168\"}, \"dim_3\": {\"$numberDecimal\": \"0.001821489303917318006496822184459403\"}, \"dim_4\": {\"$numberDecimal\": \"0.1456937654656285834993059327660088\"}}}"),
							Document.parse("{\"_id\": \"new_c_4\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.2361428571428571428571428571428571\"}, \"dim_1\": {\"$numberDecimal\": \"0.6377857142857142857142857142857143\"}, \"dim_2\": {\"$numberDecimal\": \"0.9495255411255411255411255411255417\"}, \"dim_3\": {\"$numberDecimal\": \"0.00819326630102008788177068570681056\"}, \"dim_4\": {\"$numberDecimal\": \"0.1504780010863661053775122216186852\"}}}"),
							Document.parse("{\"_id\": \"new_c_66\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.2911858974358974358974358974358974\"}, \"dim_1\": {\"$numberDecimal\": \"0.7534855769230769230769230769230769\"}, \"dim_2\": {\"$numberDecimal\": \"0.9730244755244755244755244755244734\"}, \"dim_3\": {\"$numberDecimal\": \"0.03720206547245960739544914369316013\"}, \"dim_4\": {\"$numberDecimal\": \"0.1635224724578336745637125865262746\"}}}"),
							Document.parse("{\"_id\": \"new_c_71\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.2452410383189122373300370828182942\"}, \"dim_1\": {\"$numberDecimal\": \"0.6872527812113720642768850432632880\"}, \"dim_2\": {\"$numberDecimal\": \"0.9650267820354346930366707869798064\"}, \"dim_3\": {\"$numberDecimal\": \"0.01209604367070661687583950529877653\"}, \"dim_4\": {\"$numberDecimal\": \"0.1733774504504927925853163319499742\"}}}")
						))
					);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Euclidean").append("Mean", "Geometric").
				append("Epoch0_CentroidReinitializations", 7).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_452592595547\", \"label\": \"c_5\"}"),
						Document.parse("{\"_id\": \"p_318155181\", \"label\": \"c_68\"}"),
						Document.parse("{\"_id\": \"p_340163946441\", \"label\": \"c_34\"}"),
						Document.parse("{\"_id\": \"p_825244181924\", \"label\": \"c_42\"}"),
						Document.parse("{\"_id\": \"p_169145226699\", \"label\": \"c_17\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_38\", \"sse\": {\"$numberDecimal\": \"937395.1600000000000000000000000000\"}, \"total_points\": 275}"),
						Document.parse("{\"_id\": \"c_48\", \"sse\": {\"$numberDecimal\": \"127080422.7500000000000000000000000\"}, \"total_points\": 259}"),
						Document.parse("{\"_id\": \"c_62\", \"sse\": {\"$numberDecimal\": \"89624497.39000000000000000000000000\"}, \"total_points\": 922}"),
						Document.parse("{\"_id\": \"c_5\", \"sse\": {\"$numberDecimal\": \"39859313525.80000000000000000000000\"}, \"total_points\": 4611}"),
						Document.parse("{\"_id\": \"c_15\", \"sse\": {\"$numberDecimal\": \"68351297.33000000000000000000000000\"}, \"total_points\": 239}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_28\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"2000.419611580129775631885514401401\"}, \"dim_1\": {\"$numberDecimal\": \"5.851595379371602100351526872696103\"}, \"dim_2\": {\"$numberDecimal\": \"1955.957049955727241850456144876362\"}, \"dim_3\": {\"$numberDecimal\": \"1461.567121009058757800799370587382\"}, \"dim_4\": {\"$numberDecimal\": \"103.0501911899972639206321223206843\"}}}"),
						Document.parse("{\"_id\": \"new_c_21\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1997.829710083656115910928915045830\"}, \"dim_1\": {\"$numberDecimal\": \"6.145403411390994248360472222903006\"}, \"dim_2\": {\"$numberDecimal\": \"1891.210650508652530478067354088013\"}, \"dim_3\": {\"$numberDecimal\": \"4806.705649742317092972354759840061\"}, \"dim_4\": {\"$numberDecimal\": \"102.3704800283804393883977721800243\"}}}"),
						Document.parse("{\"_id\": \"new_c_8\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1997.871428378327763309829058732795\"}, \"dim_1\": {\"$numberDecimal\": \"6.165827481845331791984236796135473\"}, \"dim_2\": {\"$numberDecimal\": \"1922.556510042943578552210444190544\"}, \"dim_3\": {\"$numberDecimal\": \"2641.930624683655019138219704938130\"}, \"dim_4\": {\"$numberDecimal\": \"103.6166047191221828172623146610928\"}}}"),
						Document.parse("{\"_id\": \"new_c_62\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"2000.918694426048344623978703850584\"}, \"dim_1\": {\"$numberDecimal\": \"6.004473647881694194824789405812662\"}, \"dim_2\": {\"$numberDecimal\": \"1960.856562074405766983649244343264\"}, \"dim_3\": {\"$numberDecimal\": \"2106.327650456076167308873555762468\"}, \"dim_4\": {\"$numberDecimal\": \"101.6420090646041890666936534358340\"}}}"),
						Document.parse("{\"_id\": \"new_c_71\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1998.480447596149095087426475317379\"}, \"dim_1\": {\"$numberDecimal\": \"5.981119830600358645328293123383958\"}, \"dim_2\": {\"$numberDecimal\": \"1946.237544880436489667246504950521\"}, \"dim_3\": {\"$numberDecimal\": \"2646.681228784669734244605226656047\"}, \"dim_4\": {\"$numberDecimal\": \"104.0907141002707163365075367082518\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Euclidean").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 8).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_488763793790\", \"label\": \"c_33\"}"),
						Document.parse("{\"_id\": \"p_13607951028580\", \"label\": \"c_33\"}"),
						Document.parse("{\"_id\": \"p_459242857875\", \"label\": \"c_29\"}"),
						Document.parse("{\"_id\": \"p_1020451569\", \"label\": \"c_60\"}"),
						Document.parse("{\"_id\": \"p_3413841041999\", \"label\": \"c_33\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_42\", \"sse\": {\"$numberDecimal\": \"9.216755021889562581879746125955643\"}, \"total_points\": 133}"),
						Document.parse("{\"_id\": \"c_49\", \"sse\": {\"$numberDecimal\": \"9.734928913912990282568230314142404\"}, \"total_points\": 155}"),
						Document.parse("{\"_id\": \"c_43\", \"sse\": {\"$numberDecimal\": \"7.376831244115480258230471184204830\"}, \"total_points\": 106}"),
						Document.parse("{\"_id\": \"c_11\", \"sse\": {\"$numberDecimal\": \"10.37324200823019270394849095529204\"}, \"total_points\": 126}"),
						Document.parse("{\"_id\": \"c_21\", \"sse\": {\"$numberDecimal\": \"4.862997508256042994553641966473374\"}, \"total_points\": 52}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_16\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.3828744192134022642497218768405071\"}, \"dim_1\": {\"$numberDecimal\": \"-0.1589694391728290033374779137490998\"}, \"dim_2\": {\"$numberDecimal\": \"-0.01370150850828816930511845766083067\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01403100631914860518399304199506537\"}, \"dim_4\": {\"$numberDecimal\": \"-0.01475514089292525845983344211094424\"}}}"),
						Document.parse("{\"_id\": \"new_c_7\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.3515122929675550881699876575714013\"}, \"dim_1\": {\"$numberDecimal\": \"0.1046684345813238205827878669818000\"}, \"dim_2\": {\"$numberDecimal\": \"-0.01056771207216615178775210308517372\"}, \"dim_3\": {\"$numberDecimal\": \"0.01376010377692117128226105608979216\"}, \"dim_4\": {\"$numberDecimal\": \"0.01499234214839455003656007506248271\"}}}"),
						Document.parse("{\"_id\": \"new_c_4\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.3707132470522301030775607046793341\"}, \"dim_1\": {\"$numberDecimal\": \"-0.004793614997004827513302089573275941\"}, \"dim_2\": {\"$numberDecimal\": \"-0.02372659453337419439114354368591666\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01232716840529493273690567004957541\"}, \"dim_4\": {\"$numberDecimal\": \"-0.008707702906323773759489426177574652\"}}}"),
						Document.parse("{\"_id\": \"new_c_42\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.3605435921457330913173910497728380\"}, \"dim_1\": {\"$numberDecimal\": \"-0.1020897399247087025855982145009784\"}, \"dim_2\": {\"$numberDecimal\": \"-0.008109346284546998195526295437089481\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01101531685302938918344769733474606\"}, \"dim_4\": {\"$numberDecimal\": \"-0.02283999180347158625651144891502669\"}}}"),
						Document.parse("{\"_id\": \"new_c_47\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.4203469466859297367771944043130346\"}, \"dim_1\": {\"$numberDecimal\": \"-0.04565899961238944289791747418866038\"}, \"dim_2\": {\"$numberDecimal\": \"-0.01045542126220092321787237041474338\"}, \"dim_3\": {\"$numberDecimal\": \"-0.01508652222984219189207717001958533\"}, \"dim_4\": {\"$numberDecimal\": \"-0.02925812839971504661844289349606104\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Manhattan").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 8).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_120324413208\", \"label\": \"c_67\"}"),
						Document.parse("{\"_id\": \"p_976209476544\", \"label\": \"c_33\"}"),
						Document.parse("{\"_id\": \"p_202381562201\", \"label\": \"c_44\"}"),
						Document.parse("{\"_id\": \"p_1286784211653\", \"label\": \"c_33\"}"),
						Document.parse("{\"_id\": \"p_40042693051\", \"label\": \"c_17\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_12\", \"sse\": {\"$numberDecimal\": \"1417.196480775995000510805373310458\"}, \"total_points\": 136}"),
						Document.parse("{\"_id\": \"c_15\", \"sse\": {\"$numberDecimal\": \"2876.410934354755759205406483633677\"}, \"total_points\": 365}"),
						Document.parse("{\"_id\": \"c_8\", \"sse\": {\"$numberDecimal\": \"29300.71303279403061790132317018357\"}, \"total_points\": 1386}"),
						Document.parse("{\"_id\": \"c_55\", \"sse\": {\"$numberDecimal\": \"719.7114001257090414547472067127054\"}, \"total_points\": 124}"),
						Document.parse("{\"_id\": \"c_17\", \"sse\": {\"$numberDecimal\": \"18199.96942731151649987055840994590\"}, \"total_points\": 1717}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_44\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.2609351645425723972751769729868660\"}, \"dim_1\": {\"$numberDecimal\": \"-0.1688813705435976362105997314086162\"}, \"dim_2\": {\"$numberDecimal\": \"-0.02266887411505196226595696581213877\"}, \"dim_3\": {\"$numberDecimal\": \"-0.2639122912530520373696257028475805\"}, \"dim_4\": {\"$numberDecimal\": \"-0.9467860719399474516196591326922676\"}}}"),
						Document.parse("{\"_id\": \"new_c_12\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.3456126461731567952302376211953578\"}, \"dim_1\": {\"$numberDecimal\": \"-1.987508513196478210530200499741364\"}, \"dim_2\": {\"$numberDecimal\": \"-0.7875068865341067594045724999906404\"}, \"dim_3\": {\"$numberDecimal\": \"-0.1871981560160712779078537292630979\"}, \"dim_4\": {\"$numberDecimal\": \"-1.091352588720056774500547049825841\"}}}"),
						Document.parse("{\"_id\": \"new_c_39\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.2711193225111288518636088035915903\"}, \"dim_1\": {\"$numberDecimal\": \"0.1502909932073919002681685820158928\"}, \"dim_2\": {\"$numberDecimal\": \"-0.2314708842207473373447114013351713\"}, \"dim_3\": {\"$numberDecimal\": \"-0.2908884323422639737379030404922797\"}, \"dim_4\": {\"$numberDecimal\": \"-0.6077544726068907090424085530846262\"}}}"),
						Document.parse("{\"_id\": \"new_c_74\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.7270735888697091205268830952056734\"}, \"dim_1\": {\"$numberDecimal\": \"0.5602054597696846495306741586267884\"}, \"dim_2\": {\"$numberDecimal\": \"-0.2434014374624488413480742799024109\"}, \"dim_3\": {\"$numberDecimal\": \"0.006168574260490063154356623696487333\"}, \"dim_4\": {\"$numberDecimal\": \"0.8298961593708182574493984364928328\"}}}"),
						Document.parse("{\"_id\": \"new_c_66\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.4232400819270736264256508037832110\"}, \"dim_1\": {\"$numberDecimal\": \"0.7509981021497926878298268260044047\"}, \"dim_2\": {\"$numberDecimal\": \"-0.006034758634669781574678638010428953\"}, \"dim_3\": {\"$numberDecimal\": \"0.1342221161078133671781489229114648\"}, \"dim_4\": {\"$numberDecimal\": \"-0.02139856330932061176174403127241194\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Euclidean").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 5).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_58561132790\", \"label\": \"c_8\"}"),
						Document.parse("{\"_id\": \"p_23973736752\", \"label\": \"c_30\"}"),
						Document.parse("{\"_id\": \"p_79488189845\", \"label\": \"c_9\"}"),
						Document.parse("{\"_id\": \"p_640451699\", \"label\": \"c_5\"}"),
						Document.parse("{\"_id\": \"p_814914434\", \"label\": \"c_25\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_45\", \"sse\": {\"$numberDecimal\": \"6.252312815534768949820116219233169\"}, \"total_points\": 253}"),
						Document.parse("{\"_id\": \"c_29\", \"sse\": {\"$numberDecimal\": \"1.530118880121360913892870746391489\"}, \"total_points\": 25}"),
						Document.parse("{\"_id\": \"c_10\", \"sse\": {\"$numberDecimal\": \"1.110761247218332111190160718088086\"}, \"total_points\": 5}"),
						Document.parse("{\"_id\": \"c_2\", \"sse\": {\"$numberDecimal\": \"0.4425964502221223559521645671795633\"}, \"total_points\": 26}"),
						Document.parse("{\"_id\": \"c_33\", \"sse\": {\"$numberDecimal\": \"1.049402959454505464946980086088359\"}, \"total_points\": 36}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_22\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.9108997334803786416689642496094113\"}, \"dim_1\": {\"$numberDecimal\": \"0.8362637362637362637362637362637364\"}, \"dim_2\": {\"$numberDecimal\": \"0.8569230769230769230769230769230769\"}, \"dim_3\": {\"$numberDecimal\": \"0.4628751974723538704581358609794633\"}, \"dim_4\": {\"$numberDecimal\": \"0.04953249413591963279118939929732995\"}}}"),
						Document.parse("{\"_id\": \"new_c_6\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.9397882959836532341808193481392159\"}, \"dim_1\": {\"$numberDecimal\": \"0.6528704939919893190921228304405874\"}, \"dim_2\": {\"$numberDecimal\": \"0.7463551401869158878504672897196262\"}, \"dim_3\": {\"$numberDecimal\": \"0.2256278513531470080170084599371041\"}, \"dim_4\": {\"$numberDecimal\": \"0.002645797969759416117715126024034336\"}}}"),
						Document.parse("{\"_id\": \"new_c_7\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.9514771369610079287498642337352023\"}, \"dim_1\": {\"$numberDecimal\": \"0.6694805194805194805194805194805194\"}, \"dim_2\": {\"$numberDecimal\": \"0.7539393939393939393939393939393939\"}, \"dim_3\": {\"$numberDecimal\": \"0.2225333907798362774666092201637226\"}, \"dim_4\": {\"$numberDecimal\": \"0.004306785244708248631640183955031298\"}}}"),
						Document.parse("{\"_id\": \"new_c_20\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.8042459332781913427074717397298046\"}, \"dim_1\": {\"$numberDecimal\": \"0.8164835164835164835164835164835169\"}, \"dim_2\": {\"$numberDecimal\": \"0.6723076923076923076923076923076923\"}, \"dim_3\": {\"$numberDecimal\": \"0.1691578563616478308421436383521692\"}, \"dim_4\": {\"$numberDecimal\": \"0.0007045746687290315234499758221953375\"}}}"),
						Document.parse("{\"_id\": \"new_c_31\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.9135727328058429701765063907486264\"}, \"dim_1\": {\"$numberDecimal\": \"0.7407008086253369272237196765498667\"}, \"dim_2\": {\"$numberDecimal\": \"0.5128301886792452830188679245283019\"}, \"dim_3\": {\"$numberDecimal\": \"0.3107693224835315508658976422546126\"}, \"dim_4\": {\"$numberDecimal\": \"0.008299254812557706593849268508362013\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Manhattan").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 4).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_43540112401\", \"label\": \"c_10\"}"),
						Document.parse("{\"_id\": \"p_6862927947\", \"label\": \"c_8\"}"),
						Document.parse("{\"_id\": \"p_52644544786\", \"label\": \"c_35\"}"),
						Document.parse("{\"_id\": \"p_761061640\", \"label\": \"c_21\"}"),
						Document.parse("{\"_id\": \"p_63186620092\", \"label\": \"c_25\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_30\", \"sse\": {\"$numberDecimal\": \"194776.3700000000000000000000000000\"}, \"total_points\": 31}"),
						Document.parse("{\"_id\": \"c_25\", \"sse\": {\"$numberDecimal\": \"44446502.81000000000000000000000000\"}, \"total_points\": 243}"),
						Document.parse("{\"_id\": \"c_46\", \"sse\": {\"$numberDecimal\": \"66692758.43000000000000000000000000\"}, \"total_points\": 202}"),
						Document.parse("{\"_id\": \"c_44\", \"sse\": {\"$numberDecimal\": \"16315803.36000000000000000000000000\"}, \"total_points\": 59}"),
						Document.parse("{\"_id\": \"c_4\", \"sse\": {\"$numberDecimal\": \"915591281218.0400000000000000000000\"}, \"total_points\": 387}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_9\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1919.388888888888888888888888888889\"}, \"dim_1\": {\"$numberDecimal\": \"6.598148148148148148148148148148148\"}, \"dim_2\": {\"$numberDecimal\": \"1964.055555555555555555555555555556\"}, \"dim_3\": {\"$numberDecimal\": \"139.4814814814814814814814814814815\"}, \"dim_4\": {\"$numberDecimal\": \"1477.166666666666666666666666666667\"}}}"),
						Document.parse("{\"_id\": \"new_c_40\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1930.3828125000000000000000000000\"}, \"dim_1\": {\"$numberDecimal\": \"6.867968750000000000000000000000\"}, \"dim_2\": {\"$numberDecimal\": \"1971.9414062500000000000000000000\"}, \"dim_3\": {\"$numberDecimal\": \"110.51171875000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"34353.824218750000000000000000000\"}}}"),
						Document.parse("{\"_id\": \"new_c_5\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1908.944444444444444444444444444444\"}, \"dim_1\": {\"$numberDecimal\": \"5.793055555555555555555555555555556\"}, \"dim_2\": {\"$numberDecimal\": \"1956.708333333333333333333333333333\"}, \"dim_3\": {\"$numberDecimal\": \"85.06944444444444444444444444444444\"}, \"dim_4\": {\"$numberDecimal\": \"1017.37500000000000000000000000000\"}}}"),
						Document.parse("{\"_id\": \"new_c_22\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1923.369791666666666666666666666667\"}, \"dim_1\": {\"$numberDecimal\": \"6.386458333333333333333333333333333\"}, \"dim_2\": {\"$numberDecimal\": \"1968.848958333333333333333333333333\"}, \"dim_3\": {\"$numberDecimal\": \"108.76562500000000000000000000000\"}, \"dim_4\": {\"$numberDecimal\": \"2616.880208333333333333333333333333\"}}}"),
						Document.parse("{\"_id\": \"new_c_4\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1944.645994832041343669250645994832\"}, \"dim_1\": {\"$numberDecimal\": \"6.716279069767441860465116279069767\"}, \"dim_2\": {\"$numberDecimal\": \"1975.054263565891472868217054263566\"}, \"dim_3\": {\"$numberDecimal\": \"110.0878552971576227390180878552972\"}, \"dim_4\": {\"$numberDecimal\": \"29948.77002583979328165374677002584\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Manhattan").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 5).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_76360553916\", \"label\": \"c_45\"}"),
						Document.parse("{\"_id\": \"p_799634334\", \"label\": \"c_9\"}"),
						Document.parse("{\"_id\": \"p_53399925380\", \"label\": \"c_30\"}"),
						Document.parse("{\"_id\": \"p_78490605363\", \"label\": \"c_9\"}"),
						Document.parse("{\"_id\": \"p_55630867391\", \"label\": \"c_30\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_37\", \"sse\": {\"$numberDecimal\": \"1.720863865301939025572981788748059\"}, \"total_points\": 57}"),
						Document.parse("{\"_id\": \"c_2\", \"sse\": {\"$numberDecimal\": \"6.862045864162511088642577536295995\"}, \"total_points\": 49}"),
						Document.parse("{\"_id\": \"c_30\", \"sse\": {\"$numberDecimal\": \"310.2524386141634912823951472488727\"}, \"total_points\": 1341}"),
						Document.parse("{\"_id\": \"c_44\", \"sse\": {\"$numberDecimal\": \"25.19917266573962249110556297124852\"}, \"total_points\": 402}"),
						Document.parse("{\"_id\": \"c_38\", \"sse\": {\"$numberDecimal\": \"0.8695021250878287206515619626181688\"}, \"total_points\": 18}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_3\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.02202171538056776647530326733537712\"}, \"dim_1\": {\"$numberDecimal\": \"0.04470187153202294742781905388883654\"}, \"dim_2\": {\"$numberDecimal\": \"-0.1486682027649769585253456221198114\"}, \"dim_3\": {\"$numberDecimal\": \"0.04245247403349047920651214162472837\"}, \"dim_4\": {\"$numberDecimal\": \"-0.002306094866934173686955695937831041\"}}}"),
						Document.parse("{\"_id\": \"new_c_15\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.005570138535987741522720386538048331\"}, \"dim_1\": {\"$numberDecimal\": \"-0.3461130178039931491516578072781655\"}, \"dim_2\": {\"$numberDecimal\": \"0.1203378334523268644122801324073515\"}, \"dim_3\": {\"$numberDecimal\": \"-0.06687839268339853258938170486654158\"}, \"dim_4\": {\"$numberDecimal\": \"-0.009897389099850763176194237562210676\"}}}"),
						Document.parse("{\"_id\": \"new_c_10\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.0335850434519975177028841366584785\"}, \"dim_1\": {\"$numberDecimal\": \"0.2742256810558324712373428634126462\"}, \"dim_2\": {\"$numberDecimal\": \"0.09742703533026113671274961597542667\"}, \"dim_3\": {\"$numberDecimal\": \"0.03193566067307522305665883537331785\"}, \"dim_4\": {\"$numberDecimal\": \"0.3229183443755413962593818560498372\"}}}"),
						Document.parse("{\"_id\": \"new_c_23\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.04131616048146448027524287372659646\"}, \"dim_1\": {\"$numberDecimal\": \"0.1484015052316566470615186875884704\"}, \"dim_2\": {\"$numberDecimal\": \"0.2433244712276970341486470518728626\"}, \"dim_3\": {\"$numberDecimal\": \"0.2031593816598293851662711819524895\"}, \"dim_4\": {\"$numberDecimal\": \"0.08072471234381007438317787849556749\"}}}"),
						Document.parse("{\"_id\": \"new_c_41\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.02333251927201681921467858735585842\"}, \"dim_1\": {\"$numberDecimal\": \"0.052130442960594375999247625317408\"}, \"dim_2\": {\"$numberDecimal\": \"0.156360368663594470046082949308760\"}, \"dim_3\": {\"$numberDecimal\": \"-0.1242096789793734341313348455113583\"}, \"dim_4\": {\"$numberDecimal\": \"-0.007851346131333611944915574248382252\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Euclidean").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 5).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_52156198231\", \"label\": \"c_15\"}"),
						Document.parse("{\"_id\": \"p_70328759162\", \"label\": \"c_9\"}"),
						Document.parse("{\"_id\": \"p_60728357824\", \"label\": \"c_5\"}"),
						Document.parse("{\"_id\": \"p_79372156982\", \"label\": \"c_47\"}"),
						Document.parse("{\"_id\": \"p_81400296902\", \"label\": \"c_9\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_49\", \"sse\": {\"$numberDecimal\": \"120.3200038859395682293910442283454\"}, \"total_points\": 107}"),
						Document.parse("{\"_id\": \"c_41\", \"sse\": {\"$numberDecimal\": \"3.013378844337211971446950082888157\"}, \"total_points\": 11}"),
						Document.parse("{\"_id\": \"c_34\", \"sse\": {\"$numberDecimal\": \"1.451548086569783763684363260693038\"}, \"total_points\": 13}"),
						Document.parse("{\"_id\": \"c_3\", \"sse\": {\"$numberDecimal\": \"142.5888503022157355399612566232075\"}, \"total_points\": 99}"),
						Document.parse("{\"_id\": \"c_13\", \"sse\": {\"$numberDecimal\": \"21.88906312217394457003094655414142\"}, \"total_points\": 8}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_37\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.8053736035845314197177756492149068\"}, \"dim_1\": {\"$numberDecimal\": \"0.8882966322727506249120761504108083\"}, \"dim_2\": {\"$numberDecimal\": \"0.1371854526097867474904330226081494\"}, \"dim_3\": {\"$numberDecimal\": \"-0.785004476810170641870162531441524\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1725189351779463530854269293525054\"}}}"),
						Document.parse("{\"_id\": \"new_c_40\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.3682924540137344934529647331852889\"}, \"dim_1\": {\"$numberDecimal\": \"1.371911716563311065285613855762142\"}, \"dim_2\": {\"$numberDecimal\": \"0.6285739723901820394188304200606383\"}, \"dim_3\": {\"$numberDecimal\": \"-0.9962864605155601478631346116212373\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1360551242345024205747472387966362\"}}}"),
						Document.parse("{\"_id\": \"new_c_35\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.9422504193921314104819162253676328\"}, \"dim_1\": {\"$numberDecimal\": \"0.3727447027931909101742482192343878\"}, \"dim_2\": {\"$numberDecimal\": \"0.3611757034377237577317397271044822\"}, \"dim_3\": {\"$numberDecimal\": \"-0.4915373824939558958341614810902108\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1339100903314881592740634731006782\"}}}"),
						Document.parse("{\"_id\": \"new_c_41\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.6531930786671836030040258111315762\"}, \"dim_1\": {\"$numberDecimal\": \"1.292276985702429757779851214743741\"}, \"dim_2\": {\"$numberDecimal\": \"0.679785679166302640562732878355935\"}, \"dim_3\": {\"$numberDecimal\": \"-1.053583269656004759657838904551329\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1463152092220561828054377170549319\"}}}"),
						Document.parse("{\"_id\": \"new_c_11\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.06185562549303923081197827320845378\"}, \"dim_1\": {\"$numberDecimal\": \"2.132312792491865216816333518264239\"}, \"dim_2\": {\"$numberDecimal\": \"0.1058894095799352690136037425388016\"}, \"dim_3\": {\"$numberDecimal\": \"0.3004517407949189482406243512896564\"}, \"dim_4\": {\"$numberDecimal\": \"1.020085565206925302170584537055711\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Manhattan").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 7).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_286751441964\", \"label\": \"c_48\"}"),
						Document.parse("{\"_id\": \"p_1004031310\", \"label\": \"c_80\"}"),
						Document.parse("{\"_id\": \"p_58530526515\", \"label\": \"c_81\"}"),
						Document.parse("{\"_id\": \"p_12080493337\", \"label\": \"c_48\"}"),
						Document.parse("{\"_id\": \"p_80100448\", \"label\": \"c_68\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_8\", \"sse\": {\"$numberDecimal\": \"3.012209846692572872322357823941650\"}, \"total_points\": 50}"),
						Document.parse("{\"_id\": \"c_99\", \"sse\": {\"$numberDecimal\": \"1.192803373526097120274166130275667\"}, \"total_points\": 23}"),
						Document.parse("{\"_id\": \"c_61\", \"sse\": {\"$numberDecimal\": \"8.734744801467600247792336524757144\"}, \"total_points\": 99}"),
						Document.parse("{\"_id\": \"c_45\", \"sse\": {\"$numberDecimal\": \"2.275233887339039982391842327529773\"}, \"total_points\": 39}"),
						Document.parse("{\"_id\": \"c_96\", \"sse\": {\"$numberDecimal\": \"1.022670138615139095481189122394712\"}, \"total_points\": 17}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_35\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.4484261501210653753026634382566586\"}, \"dim_1\": {\"$numberDecimal\": \"0.2528442071047132574878105409797986\"}, \"dim_2\": {\"$numberDecimal\": \"0.8334793372690916015996952961340697\"}, \"dim_3\": {\"$numberDecimal\": \"0.006554305767020841087968788980742467\"}, \"dim_4\": {\"$numberDecimal\": \"0.2769157316782048221532585342563851\"}}}"),
						Document.parse("{\"_id\": \"new_c_63\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.9618315018315018315018315018315082\"}, \"dim_1\": {\"$numberDecimal\": \"0.6259922725676150333684580259922749\"}, \"dim_2\": {\"$numberDecimal\": \"0.9577412849322961682512244309997015\"}, \"dim_3\": {\"$numberDecimal\": \"0.05774635277435131292888588244231190\"}, \"dim_4\": {\"$numberDecimal\": \"0.4775370169736366919465511014806786\"}}}"),
						Document.parse("{\"_id\": \"new_c_76\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.8719327731092436974789915966386548\"}, \"dim_1\": {\"$numberDecimal\": \"0.6559226430298146655922643029814681\"}, \"dim_2\": {\"$numberDecimal\": \"0.9123595505617977528089887640449441\"}, \"dim_3\": {\"$numberDecimal\": \"0.1471688557858220320283023986861665\"}, \"dim_4\": {\"$numberDecimal\": \"0.7327257663628831814415907207953602\"}}}"),
						Document.parse("{\"_id\": \"new_c_73\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.9051948051948051948051948051948045\"}, \"dim_1\": {\"$numberDecimal\": \"0.6973848069738480697384806973848077\"}, \"dim_2\": {\"$numberDecimal\": \"0.9392236976506639427987742594484168\"}, \"dim_3\": {\"$numberDecimal\": \"0.1246296499393430768072195057143129\"}, \"dim_4\": {\"$numberDecimal\": \"0.7384763124199743918053777208706782\"}}}"),
						Document.parse("{\"_id\": \"new_c_27\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.3914285714285714285714285714285716\"}, \"dim_1\": {\"$numberDecimal\": \"0.424657534246575342465753424657534\"}, \"dim_2\": {\"$numberDecimal\": \"0.836404494382022471910112359550562\"}, \"dim_3\": {\"$numberDecimal\": \"0.00617206802125573807258639213147014\"}, \"dim_4\": {\"$numberDecimal\": \"0.2605633802816901408450704225352112\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Euclidean").append("Mean", "Geometric").
				append("Epoch0_CentroidReinitializations", 6).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_356618224187\", \"label\": \"c_82\"}"),
						Document.parse("{\"_id\": \"p_17311412794962\", \"label\": \"c_10\"}"),
						Document.parse("{\"_id\": \"p_58100431837\", \"label\": \"c_45\"}"),
						Document.parse("{\"_id\": \"p_30812682984994\", \"label\": \"c_37\"}"),
						Document.parse("{\"_id\": \"p_82559307520\", \"label\": \"c_45\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_97\", \"sse\": {\"$numberDecimal\": \"1347308818.710000000000000000000000\"}, \"total_points\": 19}"),
						Document.parse("{\"_id\": \"c_36\", \"sse\": {\"$numberDecimal\": \"5528738.950000000000000000000000002\"}, \"total_points\": 13}"),
						Document.parse("{\"_id\": \"c_85\", \"sse\": {\"$numberDecimal\": \"487561.6799999999999999999999999999\"}, \"total_points\": 6}"),
						Document.parse("{\"_id\": \"c_61\", \"sse\": {\"$numberDecimal\": \"460176434824.9500000000000000000000\"}, \"total_points\": 146}"),
						Document.parse("{\"_id\": \"c_78\", \"sse\": {\"$numberDecimal\": \"2693727607.740000000000000000000000\"}, \"total_points\": 30}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_36\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1989.142692947729524431203248892181\"}, \"dim_1\": {\"$numberDecimal\": \"6.245878032505836116408850905480658\"}, \"dim_2\": {\"$numberDecimal\": \"1954.615324065102677259138316062821\"}, \"dim_3\": {\"$numberDecimal\": \"7305.130438056084640238351078281872\"}, \"dim_4\": {\"$numberDecimal\": \"66.70998009703293826561238517235843\"}}}"),
						Document.parse("{\"_id\": \"new_c_72\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"2008.487268980825921055644133726860\"}, \"dim_1\": {\"$numberDecimal\": \"5.322471006150202824563709036109307\"}, \"dim_2\": {\"$numberDecimal\": \"1977.374308477516115420011816666554\"}, \"dim_3\": {\"$numberDecimal\": \"24321.00533717368711831347595953416\"}, \"dim_4\": {\"$numberDecimal\": \"95.19926322006947644479244729217509\"}}}"),
						Document.parse("{\"_id\": \"new_c_47\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1983.032518583463709002319147752142\"}, \"dim_1\": {\"$numberDecimal\": \"5.940153336416835036404530627893537\"}, \"dim_2\": {\"$numberDecimal\": \"1935.697982893572917375596101838278\"}, \"dim_3\": {\"$numberDecimal\": \"9066.998751988916268635042656243988\"}, \"dim_4\": {\"$numberDecimal\": \"100.7144288327121335696339004182039\"}}}"),
						Document.parse("{\"_id\": \"new_c_77\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"1980.698374717367132651349843170182\"}, \"dim_1\": {\"$numberDecimal\": \"6.239460061968807766182783377023071\"}, \"dim_2\": {\"$numberDecimal\": \"1933.664855491597232831030448875415\"}, \"dim_3\": {\"$numberDecimal\": \"25792.85581378225461827158662748430\"}, \"dim_4\": {\"$numberDecimal\": \"99.13890901775119077526670270113326\"}}}"),
						Document.parse("{\"_id\": \"new_c_76\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"2003.851791331261240763198780558876\"}, \"dim_1\": {\"$numberDecimal\": \"5.413033305202584993355264191756137\"}, \"dim_2\": {\"$numberDecimal\": \"1969.872671731161658386141942652626\"}, \"dim_3\": {\"$numberDecimal\": \"25231.18243118268780576475377912583\"}, \"dim_4\": {\"$numberDecimal\": \"98.20357222264564376407890129912226\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Euclidean").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 7).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_609551239\", \"label\": \"c_81\"}"),
						Document.parse("{\"_id\": \"p_149261326040\", \"label\": \"c_40\"}"),
						Document.parse("{\"_id\": \"p_40467841226871\", \"label\": \"c_6\"}"),
						Document.parse("{\"_id\": \"p_39496583533685\", \"label\": \"c_55\"}"),
						Document.parse("{\"_id\": \"p_49169946753\", \"label\": \"c_70\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_66\", \"sse\": {\"$numberDecimal\": \"1.703992153836004206375062769528168\"}, \"total_points\": 48}"),
						Document.parse("{\"_id\": \"c_20\", \"sse\": {\"$numberDecimal\": \"0.4991412555964062901262199605143246\"}, \"total_points\": 31}"),
						Document.parse("{\"_id\": \"c_12\", \"sse\": {\"$numberDecimal\": \"1.065747430866042461737036346989050\"}, \"total_points\": 20}"),
						Document.parse("{\"_id\": \"c_78\", \"sse\": {\"$numberDecimal\": \"8.277535105915076010410496361955125\"}, \"total_points\": 79}"),
						Document.parse("{\"_id\": \"c_95\", \"sse\": {\"$numberDecimal\": \"1.809512369699170318039429027119758\"}, \"total_points\": 39}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_90\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.01639110569600974718245507158087756\"}, \"dim_1\": {\"$numberDecimal\": \"0.3001104068697607851155182989163769\"}, \"dim_2\": {\"$numberDecimal\": \"-0.002876164921779545291200498311014126\"}, \"dim_3\": {\"$numberDecimal\": \"0.388464679746709506859247047516136\"}, \"dim_4\": {\"$numberDecimal\": \"0.1701202738820985615183639148322769\"}}}"),
						Document.parse("{\"_id\": \"new_c_98\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.1493787269740291516655597888776324\"}, \"dim_1\": {\"$numberDecimal\": \"0.07464442887739608715908443410613664\"}, \"dim_2\": {\"$numberDecimal\": \"0.007749312014251820653593333754875408\"}, \"dim_3\": {\"$numberDecimal\": \"0.02160399258776162087564510717263164\"}, \"dim_4\": {\"$numberDecimal\": \"-0.1349066087250195570289857569690226\"}}}"),
						Document.parse("{\"_id\": \"new_c_86\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.02479627834851715448730374103507789\"}, \"dim_1\": {\"$numberDecimal\": \"-0.2263528558948718425308079775468857\"}, \"dim_2\": {\"$numberDecimal\": \"0.01354729925449361974600948271919163\"}, \"dim_3\": {\"$numberDecimal\": \"-0.03952677263742961559482640911480982\"}, \"dim_4\": {\"$numberDecimal\": \"-0.02527939686968486632140195310260514\"}}}"),
						Document.parse("{\"_id\": \"new_c_5\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.09888885244109124706139631512765210\"}, \"dim_1\": {\"$numberDecimal\": \"0.09146973247566173348222009027570244\"}, \"dim_2\": {\"$numberDecimal\": \"0.01101855459091749549572792906685147\"}, \"dim_3\": {\"$numberDecimal\": \"-0.03660397416003217928000025262147421\"}, \"dim_4\": {\"$numberDecimal\": \"-0.08064070223099022762676325846391041\"}}}"),
						Document.parse("{\"_id\": \"new_c_75\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.1431690896168508108806616269302901\"}, \"dim_1\": {\"$numberDecimal\": \"-0.01537319628507524643284201656722616\"}, \"dim_2\": {\"$numberDecimal\": \"0.01043187535255286387266933971612728\"}, \"dim_3\": {\"$numberDecimal\": \"0.04781496277868279686041259626292215\"}, \"dim_4\": {\"$numberDecimal\": \"0.3207513329638618686337837063084064\"}}}")
					))
				);
		
		settings.add(new Document().append("MaxMinutes", 15).append("Distance", "Manhattan").append("Mean", "Arithmetic").
				append("Epoch0_CentroidReinitializations", 7).
				append("Epoch0_ExpectedSamples", Lists.newArrayList(
						Document.parse("{\"_id\": \"p_93171413459\", \"label\": \"c_68\"}"),
						Document.parse("{\"_id\": \"p_2467046481542\", \"label\": \"c_87\"}"),
						Document.parse("{\"_id\": \"p_2686951602\", \"label\": \"c_40\"}"),
						Document.parse("{\"_id\": \"p_105569437099\", \"label\": \"c_19\"}"),
						Document.parse("{\"_id\": \"p_104850331\", \"label\": \"c_23\"}")
					)).
				append("Epoch0_ExpectedCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"c_13\", \"sse\": {\"$numberDecimal\": \"593.1050031002464121063828463373640\"}, \"total_points\": 51}"),
						Document.parse("{\"_id\": \"c_78\", \"sse\": {\"$numberDecimal\": \"1055.378545805954340794112061516016\"}, \"total_points\": 100}"),
						Document.parse("{\"_id\": \"c_86\", \"sse\": {\"$numberDecimal\": \"44.90525198498374425442767524185473\"}, \"total_points\": 27}"),
						Document.parse("{\"_id\": \"c_82\", \"sse\": {\"$numberDecimal\": \"221.4562492105451342741036686241128\"}, \"total_points\": 78}"),
						Document.parse("{\"_id\": \"c_91\", \"sse\": {\"$numberDecimal\": \"69.80391932083757692666274745404208\"}, \"total_points\": 8}")
						)).
				append("Epoch0_ExpectedNewCentroids", Lists.newArrayList(
						Document.parse("{\"_id\": \"new_c_49\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.4209611327350969646792869361467859\"}, \"dim_1\": {\"$numberDecimal\": \"0.5290204059563132021197341497639195\"}, \"dim_2\": {\"$numberDecimal\": \"-0.07736026272393073882136267687078839\"}, \"dim_3\": {\"$numberDecimal\": \"-0.3499211466661456321250132976579328\"}, \"dim_4\": {\"$numberDecimal\": \"-0.4578584748755062551829130532261021\"}}}"),
						Document.parse("{\"_id\": \"new_c_60\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"0.8480548888374403423534909857866852\"}, \"dim_1\": {\"$numberDecimal\": \"0.08707701867300682489915271055927575\"}, \"dim_2\": {\"$numberDecimal\": \"0.5268941767497998761309173614072689\"}, \"dim_3\": {\"$numberDecimal\": \"0.2495012957919629574658049012468593\"}, \"dim_4\": {\"$numberDecimal\": \"0.8494373408845765192929418356607620\"}}}"),
						Document.parse("{\"_id\": \"new_c_1\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-0.2921256787324136119290928079097164\"}, \"dim_1\": {\"$numberDecimal\": \"1.175912221896148005207553077139829\"}, \"dim_2\": {\"$numberDecimal\": \"-0.5894774742130735207934619621747756\"}, \"dim_3\": {\"$numberDecimal\": \"1.193292965590163513396812927837771\"}, \"dim_4\": {\"$numberDecimal\": \"0.72856103610236988833759023377030\"}}}"),
						Document.parse("{\"_id\": \"new_c_25\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-1.563285382775816093002785524988358\"}, \"dim_1\": {\"$numberDecimal\": \"-0.0277070856170643585073017601929106\"}, \"dim_2\": {\"$numberDecimal\": \"-1.690404791639641343776509280226695\"}, \"dim_3\": {\"$numberDecimal\": \"-0.5165114540405261717495208360521224\"}, \"dim_4\": {\"$numberDecimal\": \"-0.8369313754947579832149192169629823\"}}}"),
						Document.parse("{\"_id\": \"new_c_47\", \"centroid\": {\"dim_0\": {\"$numberDecimal\": \"-1.027135019316841127035791134448836\"}, \"dim_1\": {\"$numberDecimal\": \"0.5526214478396292361955744268347212\"}, \"dim_2\": {\"$numberDecimal\": \"-0.6989620999829594083139400014862422\"}, \"dim_3\": {\"$numberDecimal\": \"-0.4624868375572166985781393452675483\"}, \"dim_4\": {\"$numberDecimal\": \"-0.5432265454423285973821715640711737\"}}}")
					))
				);
	}


	public static void main(String[] args) {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String folderToJSONFiles = args[2];
		final String folderToAssignment = args[3];
		final boolean windows = Boolean.valueOf(args[4]);
		
		// Name of the folder containing the Gradle project.
		final String folderToSearch = "AssignPointsToCentroids";
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
						String mongoCol = "Points_"+current;
						
						// Destroy the existing collection!
						client = getClient(mongoDBURL);
						db = client.getDatabase(mongoDBName);
						db.getCollection(mongoCol).drop();

						MongoCollection<Document> collection = db.getCollection(mongoCol);
						
						// Read JSON file with points and load into collection.
						Scanner sc = new Scanner(new File(folderToJSONFiles + mongoCol+".json"));
						while (sc.hasNextLine())
							collection.insertOne(Document.parse(sc.nextLine()));
						sc.close();
						
						client.close();
						
						// Let's run it for a bunch of epochs.
						for (int epoch = 0; epoch < 1; epoch++) {
							client = getClient(mongoDBURL);
							db = client.getDatabase(mongoDBName);
							collection = db.getCollection(mongoCol);
							initializeCentroids(collection, epoch);
							client.close();
							
							// Let's run the program!
							ProcessBuilder builder = new ProcessBuilder(gradleProject.getAbsolutePath() + 
									"/app/build/install/app/bin/app" + (windows?".bat":""), mongoDBURL, mongoDBName, mongoCol, 
									setting.get("Distance").toString(), setting.get("Mean").toString()).redirectErrorStream(true);
							builder.environment().put("JAVA_OPTS", "-Xmx" + maxMem + "m");
					
							Process process = null;
							try {
								long before = System.nanoTime();
								// Start the process.
								process = builder.start();
								// Change false/true to print to console the output of the program.
								StreamGobbler gobbler = new GradeAssignPoints().new StreamGobbler(process.getInputStream(), false);
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
									long cnt = 0;
									if ((cnt = collection.countDocuments(Document.parse("{_id : /^c\\_.*/, reinitialize:true}"))) != 
											setting.getInteger("Epoch"+epoch+"_CentroidReinitializations")) {
										System.out.println("The total number of centroid reinitializations were not as expected: "+cnt + 
												" instead of " + setting.getInteger("Epoch"+epoch+"_CentroidReinitializations"));
										System.out.println("\tPenalty: -10");
									}
									
									for (Document doc : setting.getList("Epoch"+epoch+"_ExpectedSamples", Document.class)) {
										Document other = collection.find(new Document().append("_id", doc.get("_id"))).first();
										if (other != null)
											other.remove("point");
										if (other == null || !Maps.difference(doc, other).areEqual()) {
											System.out.println("Expected example is: " + doc);
											System.out.println("Example retrieved: " + other);
											System.out.println("\tPenalty: -5");
										}
									}
									
									for (Document doc : setting.getList("Epoch"+epoch+"_ExpectedCentroids", Document.class)) {
										Document other = collection.find(new Document().append("_id", doc.get("_id"))).first();
										if (other != null) {
											other.remove("centroid");
											other.remove("reinitialize");
										}
										if (other == null || !Maps.difference(doc, other).areEqual()) {
											System.out.println("Expected centroid is: " + doc);
											System.out.println("Centroid retrieved: " + other);
											System.out.println("\tPenalty: -5");
										}
									}
									
									for (Document doc : setting.getList("Epoch"+epoch+"_ExpectedNewCentroids", Document.class)) {
										Document other = collection.find(new Document().append("_id", doc.get("_id"))).first();
										if (other == null || !Maps.difference(doc, other).areEqual()) {
											System.out.println("Expected new centroid is: " + doc);
											System.out.println("New centroid retrieved: " + other);
											System.out.println("\tPenalty: -5");
										}
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
	
	private static void initializeCentroids(MongoCollection<Document> collection, int epoch) {
		long processed = 0;
		for (Document c : collection.find(Document.parse("{_id:/^c\\_.*/}")).sort(Document.parse("{_id:1}"))) {
			if (!c.get("centroid", Document.class).containsKey("dim_0") || c.getBoolean("reinitialize", false)) {
				Document centroid = new Document();
				
				if (processed == 0) {
					Document limits = collection.find(Document.parse("{_id:'limits'}")).first();
					for (int i = 0; ; i++)
						if (limits.containsKey("dim_"+i))
							centroid.append("dim_"+i, limits.get("dim_"+i, Document.class).get("max", Decimal128.class));
						else
							break;
				} else {
					Document s = collection.aggregate(Lists.newArrayList(Document.parse("{$match:{_id:/^p\\_.*/}}"), 
							Document.parse("{$sort : {_id:1}}"), Document.parse("{ $skip : "+(((epoch+1)*25)+processed)+" }"))).first();
					centroid = s.get("point", Document.class);
				}
				
				collection.updateOne(new Document().append("_id", c.get("_id")), 
						new Document().append("$set", new Document().append("centroid", centroid)));
			}
				
			processed++;
		}
		
		for (Document new_c : collection.find(Document.parse("{_id:/^new\\_c\\_.*/}")).sort(Document.parse("{_id:1}"))) {
			new_c.put("_id", new_c.getString("_id").replace("new_", ""));
			collection.replaceOne(new Document().append("_id", new_c.getString("_id")), new_c);
		}
		
		collection.deleteMany(Document.parse("{_id:/^new\\_c\\_.*/}"));
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
			StreamGobbler gobbler = new GradeAssignPoints().new StreamGobbler(process.getInputStream(), true);
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
