package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;

import com.google.common.io.Files;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;
import com.logimethods.connector.nats_spark.NatsSparkUtilities;

public class AbstractSparkToNatsConnectorTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected static final String DEFAULT_SUBJECT = "spark2natsSubject";
	protected static JavaStreamingContext ssc;
	protected static Logger logger = null;
//	protected File tempDir;
	protected static DataSource dataSource;
	static {
		switch (UnitTestUtilities.getProperty("data_source", "file")) {
			case "file": dataSource = new DataSourceFile() ;
						 break;
			case "socket": dataSource = 
								new DataSourceSocket(UnitTestUtilities.getProperty("socket_hostname_write", "localhost"),
										UnitTestUtilities.getIntProperty("socket_port_write", 9998),
										UnitTestUtilities.getProperty("socket_hostname_read", "localhost"),
										UnitTestUtilities.getIntProperty("socket_port_read", 9999)) ;
			 			 break;
		}
	}
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
		// To avoid "Only one StreamingContext may be started in this JVM. Currently running StreamingContext was started at .../..."
		Thread.sleep(500);
		
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = 
				UnitTestUtilities.newSparkConf()
					.setAppName("AbstractSparkToNatsConnector");
		ssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
//	    tempDir = Files.createTempDir();
//	    tempDir.deleteOnExit();
		dataSource.setup();
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

	protected void writeFullData(final List<Integer> data) throws IOException {
/*		final File tmpFile = new File(tempDir.getAbsolutePath(), "tmp" + fileTmpIncr++ +".txt");
		final PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(Integer str: data) {
			writer.println(str);
		}		
		writer.close();*/
		dataSource.open();
		dataSource.write(data);
		dataSource.close();
	}
}
