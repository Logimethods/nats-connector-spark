/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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
import com.logimethods.nats.connector.spark.StreamingNatsSubscriber;
import com.logimethods.nats.connector.spark.STANServer;
import com.logimethods.nats.connector.spark.TestClient;
import com.logimethods.nats.connector.spark.UnitTestUtilities;
import com.logimethods.nats.connector.spark.subscribe.NatsToSparkConnector;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

// Call first $~/Applications/nats-streaming-server-darwin-amd64/nats-streaming-server -m 8222 -p 4223
public class SparkToStreamingNatsConnectorPoolTest implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static final String clusterName = "test-cluster"; //"my_test_cluster";
    static final String clientName = "me";

	protected static final String DEFAULT_SUBJECT = "spark2natsStreamingSubject";
	private static final int STANServerPORT = 4223;
	static JavaStreamingContext ssc;
	static Logger logger = null;
	File tempDir;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		UnitTestUtilities.setLogLevel(NatsToSparkConnector.class, Level.TRACE);
		UnitTestUtilities.setLogLevel(SparkToStreamingNatsConnectorPoolTest.class, Level.TRACE);
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, Level.TRACE);		
		UnitTestUtilities.setLogLevel(TestClient.class, Level.TRACE);

		logger = LoggerFactory.getLogger(SparkToStreamingNatsConnectorPoolTest.class);       
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
	protected StreamingNatsSubscriber getStreamingNatsSubscriber(final List<String> data, String subject, String clusterName, String clientName) {
		ExecutorService executor = Executors.newFixedThreadPool(1);

		StreamingNatsSubscriber ns = new StreamingNatsSubscriber(subject + "_id", subject, clusterName, clientName, data.size());

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
        	ConnectionFactory connectionFactory = new ConnectionFactory(clusterName, getClientName());
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
    public void testStreamingSparkToNatsPublish() {
        // Run a STAN server
        try (STANServer s = runServer(clusterName, false)) {
        	logger.debug("STANServer started: " + s);
            try (Connection stanc =
                    new ConnectionFactory(clusterName, clientName).createConnection()) {
            	logger.debug("ConnectionFactory ready: " + stanc);
        		final List<String> data = getData();

        		String subject1 = "subject1";
        		StreamingNatsSubscriber ns1 = getStreamingNatsSubscriber(data, subject1, clusterName, getClientName() + "_SUB1");
            	logger.debug("ns1 StreamingNatsSubscriber ready");

        		String subject2 = "subject2";
        		StreamingNatsSubscriber ns2 = getStreamingNatsSubscriber(data, subject2, clusterName, getClientName() + "_SUB2");
            	logger.debug("ns2 StreamingNatsSubscriber ready");

        		JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

        		final SparkToNatsConnectorPool<?> connectorPool = new SparkToStreamingNatsConnectorPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2);
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
            } catch (IOException | TimeoutException | InterruptedException e) {
                fail(e.getMessage());
            }
        }
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
    
    static String getClientName() {
    	return clientName +  + (new Date().getTime());
    }
}
