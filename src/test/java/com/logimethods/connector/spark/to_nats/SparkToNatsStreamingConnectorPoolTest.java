/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
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
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnectorPool;
import com.logimethods.connector.spark.to_nats.SparkToNatsStreamingConnectorImpl;
import com.logimethods.connector.spark.to_nats.SparkToNatsStreamingConnectorPool;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import static io.nats.client.Constants.*;

// Call first $ nats-streaming-server -m 8222 -p 4223
public class SparkToNatsStreamingConnectorPoolTest implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static final String clusterName = "test-cluster"; //"my_test_cluster";
    static final String clientName = "me";

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
		Level level = Level.WARN;
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

    @Test
    public void testBasicPublish() {
        // Run a STAN server
        try (STANServer s = runServer(clusterName, false)) {
        	ConnectionFactory connectionFactory = new ConnectionFactory(clusterName, getUniqueClientName());
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
		final SparkToNatsConnectorPool<?> connectorPool = new SparkToNatsStreamingConnectorPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2).withNatsURL(STAN_URL);

		validateConnectorPool(subject1, subject2, connectorPool);
    }

    @Test(expected=IncompleteException.class)
    public void testEmptyStreamingSparkToNatsPublish() throws Exception {
		final SparkToNatsConnectorPool<?> connectorPool = new SparkToNatsStreamingConnectorPool();
		connectorPool.getConnector();
    }

    @Test(expected=IncompleteException.class)
    public void testEmptyStreamingSparkToNatsWithEmptyPropertiesPublish() throws Exception {
		final Properties properties = new Properties();
		final SparkToNatsConnectorPool<?> connectorPool = new SparkToNatsStreamingConnectorPool().withProperties(properties);
		connectorPool.getConnector();
    }

    @Test(timeout=8000)
    public void testStreamingSparkToNatsWithPROP_URLPropertiesPublish() throws InterruptedException, IOException, TimeoutException {
		String subject1 = "subject1";
		String subject2 = "subject2";
		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, STAN_URL);
		final SparkToNatsConnectorPool<?> connectorPool = 
				new SparkToNatsStreamingConnectorPool().withProperties(properties).withSubjects(DEFAULT_SUBJECT, subject1, subject2);

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
    	runServer(clusterName, false);
    	ConnectionFactory connectionFactory = new ConnectionFactory(clusterName, getUniqueClientName());
    	connectionFactory.setNatsUrl("nats://localhost:" + STANServerPORT);
    	Connection stanc = connectionFactory.createConnection();
    	logger.debug("ConnectionFactory ready: " + stanc);
    	final List<String> data = getData();

    	NatsStreamingSubscriber ns1 = getNatsStreamingSubscriber(data, subject1, clusterName, getUniqueClientName() + "_SUB1");
    	logger.debug("ns1 NatsStreamingSubscriber ready");

    	NatsStreamingSubscriber ns2 = getNatsStreamingSubscriber(data, subject2, clusterName, getUniqueClientName() + "_SUB2");
    	logger.debug("ns2 NatsStreamingSubscriber ready");

    	JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

    	lines.foreachRDD(new Function<JavaRDD<String>, Void> (){
    		@Override
    		public Void call(JavaRDD<String> rdd) throws Exception {
    			final SparkToNatsConnector<?> connector = connectorPool.getConnector();
    			rdd.foreachPartition(new VoidFunction<Iterator<String>> (){
    				@Override
    				public void call(Iterator<String> strings) throws Exception {
    					while(strings.hasNext()) {
    						final String str = strings.next();
    						logger.debug("Will publish " + str);
    						connector.publish(str);
    					}
    				}
    			});
    			connectorPool.returnConnector(connector);  // return to the pool for future reuse
    			return null;
    		}			
    	});

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
    	return clientName +  + (new Date().getTime());
    }
}
