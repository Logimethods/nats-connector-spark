/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats.api;

import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Constants.PROP_URL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.logimethods.connector.nats.spark.NatsStreamingSubscriber;
import com.logimethods.connector.nats.spark.STANServer;
import com.logimethods.connector.nats.spark.TestClient;
import com.logimethods.connector.nats.spark.UnitTestUtilities;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;
import com.logimethods.connector.nats_spark.IncompleteException;
import com.logimethods.connector.nats_spark.Utilities;
import com.logimethods.connector.spark.to_nats.AbstractSparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnectorPool;
import com.logimethods.connector.spark.to_nats.SparkToNatsStreamingConnectorImpl;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

// Call first $ nats-streaming-server -m 8222 -p 4223
public class SparkToNatsStreamingConnectorPoolTest implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static final String clusterID = "test-cluster"; //"my_test_cluster";
//    static final String clientName = "me";

	protected static final String DEFAULT_SUBJECT = "spark2natsStreamingSubject";
	private static final int STANServerPORT = 4223;
	private static final String STAN_URL = "nats://localhost:" + STANServerPORT;
	static JavaStreamingContext ssc;
	static Logger logger = null;
	File tempDir;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		Level level = Level.DEBUG;
		UnitTestUtilities.setLogLevel(SparkToNatsConnectorPool.class, level);
		UnitTestUtilities.setLogLevel(NatsToSparkConnector.class, level);
		UnitTestUtilities.setLogLevel(SparkToNatsStreamingConnectorPoolTest.class, level);
		UnitTestUtilities.setLogLevel(SparkToNatsStreamingConnectorImpl.class, level);		
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, level);		
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", level);
		UnitTestUtilities.setLogLevel("org.spark-project", level);

		logger = LoggerFactory.getLogger(SparkToNatsStreamingConnectorPoolTest.class);       
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("My Spark Streaming Job");
		ssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
	    tempDir = Files.createTempDir();
	    tempDir.deleteOnExit();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	    ssc.stop();
	    ssc = null;
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
	protected NatsStreamingSubscriber getNatsStreamingSubscriber(final List<String> data, String subject, String clusterName, String clientName) {
		ExecutorService executor = Executors.newFixedThreadPool(1);

		NatsStreamingSubscriber ns = new NatsStreamingSubscriber(STAN_URL, subject + "_id", subject, clusterName, clientName, data.size());

		// start the subscribers apps
		executor.execute(ns);

		// wait for subscribers to be ready.
		ns.waitUntilReady();
		return ns;
	}

    @Test(timeout=6000)
    public void testBasicPublish() {
        // Run a STAN server
        try (STANServer s = runServer(clusterID, false)) {
        	ConnectionFactory connectionFactory = new ConnectionFactory(clusterID, getUniqueClientName());
        	connectionFactory.setNatsUrl("nats://localhost:" + STANServerPORT);
            try ( Connection sc =
            		connectionFactory.createConnection()) {
                sc.publish("foo", "Hello World!".getBytes());
            } catch (IOException | TimeoutException e) {
                fail(e.getMessage());
            }
        }
    }

    @Test(timeout=8000)
    public void testStreamingSparkToNatsPublish() throws InterruptedException, IOException, TimeoutException {
		String subject1 = "subject1";
		String subject2 = "subject2";
		final SparkToNatsConnectorPool<?> connectorPool = 
				SparkToNatsConnectorPool.newStreamingPool(clusterID).withSubjects(DEFAULT_SUBJECT, subject1, subject2).withNatsURL(STAN_URL);

		validateConnectorPool(subject1, subject2, connectorPool);
    }

    @Test(expected=IncompleteException.class)
    public void testEmptyStreamingSparkToNatsPublish() throws Exception {
		final SparkToNatsConnectorPool<?> connectorPool = SparkToNatsConnectorPool.newStreamingPool(clusterID);
		connectorPool.getConnector();
    }

    @Test(expected=IncompleteException.class)
    public void testEmptyStreamingSparkToNatsWithEmptyPropertiesPublish() throws Exception {
		final Properties properties = new Properties();
		final SparkToNatsConnectorPool<?> connectorPool = SparkToNatsConnectorPool.newStreamingPool(clusterID).withProperties(properties);
		connectorPool.getConnector();
    }

    @Test(timeout=8000)
    public void testStreamingSparkToNatsWithPROP_URLPropertiesPublish() throws InterruptedException, IOException, TimeoutException {
		String subject1 = "subject1";
		String subject2 = "subject2";
		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, STAN_URL);
		final SparkToNatsConnectorPool<?> connectorPool = 
				SparkToNatsConnectorPool.newStreamingPool(clusterID).withProperties(properties).withSubjects(DEFAULT_SUBJECT, subject1, subject2);

		validateConnectorPool(subject1, subject2, connectorPool);
    }

    @Test(timeout=10000)
    public void testStreamingSparkToNatsWithConnectionTimeout() throws InterruptedException, IOException, TimeoutException {
    	boolean recordConnections = AbstractSparkToNatsConnector.recordConnections;
    	AbstractSparkToNatsConnector.recordConnections = true;
    	SparkToNatsConnector.CONNECTIONS.clear();
    	
		String subject1 = "subject1";
		String subject2 = "subject2";
		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, STAN_URL);
		final SparkToNatsConnectorPool<?> connectorPool = 
				SparkToNatsConnectorPool
					.newStreamingPool(clusterID)
					.withProperties(properties)
					.withConnectionTimeout(Duration.ofSeconds(2))
					.withSubjects(DEFAULT_SUBJECT, subject1, subject2);


    	assertTrue("NO connections should be opened when entering the test", SparkToNatsConnector.CONNECTIONS.isEmpty());

		validateConnectorPool(subject1, subject2, connectorPool);
    	
    	assertFalse("Some connections should have been opened", SparkToNatsConnector.CONNECTIONS.isEmpty());
		
		TimeUnit.SECONDS.sleep(6);
		
		assertTrue("NO connections should be still opened when exiting the test", SparkToNatsConnector.CONNECTIONS.isEmpty());
		
		AbstractSparkToNatsConnector.recordConnections = recordConnections;
    }

    @Test(timeout=8000)
    public void testStreamingSparkToNatsWithFullPropertiesPublish() throws InterruptedException, IOException, TimeoutException {
		String subject1 = "subject1";
		String subject2 = "subject2";
		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, STAN_URL);
		properties.setProperty(PROP_SUBJECTS, subject1 + ","+DEFAULT_SUBJECT+" , "+subject2);
		final SparkToNatsConnectorPool<?> connectorPool = 
				SparkToNatsConnectorPool.newStreamingPool(clusterID).withProperties(properties);

		validateConnectorPool(subject1, subject2, connectorPool);
    }

	/**
	 * @param subject1
	 * @param subject2
	 * @param connectorPool
	 * @throws InterruptedException 
	 * @throws TimeoutException 
	 * @throws IOException 
	 */
    protected void validateConnectorPool(String subject1, String subject2,
    		final SparkToNatsConnectorPool<?> connectorPool) throws InterruptedException, IOException, TimeoutException {
    	
        // Run a STAN server
    	runServer(clusterID, false);
    	ConnectionFactory connectionFactory = new ConnectionFactory(clusterID, getUniqueClientName());
    	connectionFactory.setNatsUrl("nats://localhost:" + STANServerPORT);
    	Connection stanc = connectionFactory.createConnection();
    	logger.debug("ConnectionFactory ready: " + stanc);
    	final List<String> data = getData();

    	NatsStreamingSubscriber ns1 = getNatsStreamingSubscriber(data, subject1, clusterID, getUniqueClientName() + "_SUB1");
    	logger.debug("ns1 NatsStreamingSubscriber ready");

    	NatsStreamingSubscriber ns2 = getNatsStreamingSubscriber(data, subject2, clusterID, getUniqueClientName() + "_SUB2");
    	logger.debug("ns2 NatsStreamingSubscriber ready");

    	JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

    	connectorPool.publishToNats(lines);

    	ssc.start();

    	Thread.sleep(1000);

    	File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
    	PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
    	for(String str: data) {
    		writer.println(str);
    	}		
    	writer.close();

    	// wait for the subscribers to complete.
    	ns1.waitForCompletion();
    	ns2.waitForCompletion();
    }
    
    static STANServer runServer(String clusterID) {
        return runServer(clusterID, false);
    }

    static STANServer runServer(String clusterID, boolean debug) {
        STANServer srv = new STANServer(clusterID, STANServerPORT, debug);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return srv;
    }
    
    static String getUniqueClientName() {
    	return "clientName_" + Utilities.generateUniqueID();
    }
}
