package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats.spark.UnitTestUtilities.NATS_SERVER_URL;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.logimethods.connector.nats.spark.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.TestClient;
import com.logimethods.connector.nats.spark.UnitTestUtilities;

public class AbstractSparkToNatsConnectorTest implements Serializable {

	protected static final String DEFAULT_SUBJECT = "spark2natsSubject";
	protected static JavaStreamingContext ssc;
	protected static Logger logger = null;
	protected File tempDir;
	protected int fileTmpIncr = 0;

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		UnitTestUtilities.stopDefaultServer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("My Spark Streaming Job");
		ssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
	    tempDir = Files.createTempDir();
	    tempDir.deleteOnExit();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	    if (ssc != null) {
			ssc.stop();
			ssc = null;
		}
	}

	/**
	 * @return
	 */
	protected List<String> getData() {
		final List<String> data = Arrays.asList(new String[] {
				"data_1",
				"data_2",
				"data_3",
				"data_4",
				"data_5",
				"data_6"
		});
		return data;
	}

	/**
	 * @param data
	 * @return
	 */
	protected StandardNatsSubscriber getStandardNatsSubscriber(final List<String> data, String subject) {
		ExecutorService executor = Executors.newFixedThreadPool(1);
	
		final StandardNatsSubscriber ns = new StandardNatsSubscriber(NATS_SERVER_URL, subject + "_id", subject, data.size());
	
		// start the subscribers apps
		executor.execute(ns);
	
		// wait for subscribers to be ready.
		ns.waitUntilReady();
		return ns;
	}

	protected void writeTmpFile(final List<String> data) throws FileNotFoundException, UnsupportedEncodingException {
		final File tmpFile = new File(tempDir.getAbsolutePath(), "tmp" + fileTmpIncr++ +".txt");
		final PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();
	}

}
