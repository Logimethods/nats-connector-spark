/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.SPARK_MASTER;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class UnitTestUtilities {
	protected static final Logger LOGGER = LoggerFactory.getLogger(UnitTestUtilities.class);

	public final static String SPARK_MASTER;

	public final static String NATS_STREAMING_SERVER;
    public final static int NATS_STREAMING_PORT;
	
	public final static String NATS_SERVER;
	public final static int NATS_PORT;
	
	protected static final Properties properties = new Properties();

	protected static final String DEFAULT_TEST_MODE = "local";	
	public static enum TestMode {local, cluster};
	protected static TestMode TEST_MODE;	
	
	static {
		String testModeStr = System.getenv("TEST_MODE");
		testModeStr = testModeStr != null ? testModeStr : DEFAULT_TEST_MODE;
		TEST_MODE = TestMode.valueOf(testModeStr);
		LOGGER.info("TEST_MODE: " + testModeStr);
        InputStream stream = UnitTestUtilities.class.getResourceAsStream("/config_" + testModeStr + ".properties");        
        try {
            properties.load(stream);
            LOGGER.info("PROPERTIES: " + properties.toString());
//            System.out.println("PROPERTIES " + properties.toString());
        } catch (IOException e) {
            e.printStackTrace();
            // You will have to take some action here...
        }
        
        SPARK_MASTER = properties.getProperty("spark_master", "local[2]");

        NATS_SERVER = properties.getProperty("nats_server", "localhost");
        NATS_PORT = Integer.valueOf(properties.getProperty("nats_port", "4222"));
        
        NATS_STREAMING_SERVER = properties.getProperty("nats_streaming_server", "localhost");
        NATS_STREAMING_PORT = Integer.valueOf(properties.getProperty("nats_steaming_port", "4222"));
    }

	private static final String ORG_SLF4J_SIMPLE_LOGGER_LOG = "org.slf4j.simpleLogger.log.";
	static NATSServer defaultServer = null;

	public static final String NATS_URL = "nats://"+NATS_SERVER+":"+NATS_PORT;
	public static final String NATS_STREAMING_URL = "nats://"+NATS_STREAMING_SERVER+":"+NATS_STREAMING_PORT;	

	public static final String NATS_LOCALHOST_URL = "nats://localhost:"+NATS_PORT;
	public static final String NATS_STREAMING_LOCALHOST_URL = "nats://localhost:"+NATS_STREAMING_PORT;	

	public static final String CLUSTER_ID = "test-cluster";

	Process authServerProcess = null;

	public static final String getProperty(String key, String defaultValue) {
		return properties.getProperty(key, defaultValue);
	}
	
	public static int getIntProperty(String key, int defaultValue) {
		final String valueStr = properties.getProperty(key);
		return (valueStr != null) ? Integer.valueOf(valueStr) : defaultValue;
	}

	public static synchronized void startDefaultServer() {
		if (defaultServer == null) {
			defaultServer = new NATSServer(NATS_PORT);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
		}
	}

	public static synchronized void stopDefaultServer() {
		if (defaultServer != null) {
			defaultServer.shutdown();
			defaultServer = null;
		}
	}

    public static STANServer startStreamingServer(String clusterID, boolean debug) {
        STANServer srv = new STANServer(clusterID, NATS_STREAMING_PORT, debug);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return srv;
    }
    
	public static STANServer startStreamingServer(String clusterID) {
        return startStreamingServer(clusterID, false);
    }

	public static synchronized void bounceDefaultServer(int delayMillis) {
		stopDefaultServer();
		try {
			Thread.sleep(delayMillis);
		} catch (InterruptedException e) {
			// NOOP
		}
		startDefaultServer();
	}

	public void startAuthServer() throws IOException {
		authServerProcess = Runtime.getRuntime().exec("gnatsd -config auth.conf");
	}

	NATSServer createServerOnPort(int p) {
		NATSServer n = new NATSServer(p);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}
		return n;
	}

	NATSServer createServerWithConfig(String configFile) {
		NATSServer n = new NATSServer(configFile);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}
		return n;
	}

	public static String getCommandOutput(String command) {
		String output = null;       //the string to return

		Process process = null;
		BufferedReader reader = null;
		InputStreamReader streamReader = null;
		InputStream stream = null;

		try {
			process = Runtime.getRuntime().exec(command);

			//Get stream of the console running the command
			stream = process.getInputStream();
			streamReader = new InputStreamReader(stream);
			reader = new BufferedReader(streamReader);

			String currentLine = null;  //store current line of output from the cmd
			StringBuilder commandOutput = new StringBuilder();  //build up the output from cmd
			while ((currentLine = reader.readLine()) != null) {
				commandOutput.append(currentLine + "\n");
			}

			int returnCode = process.waitFor();
			if (returnCode == 0) {
				output = commandOutput.toString();
			}

		} catch (IOException e) {
			System.err.println("Cannot retrieve output of command");
			System.err.println(e);
			output = null;
		} catch (InterruptedException e) {
			System.err.println("Cannot retrieve output of command");
			System.err.println(e);
		} finally {
			//Close all inputs / readers

			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					System.err.println("Cannot close stream input! " + e);
				}
			}
			if (streamReader != null) {
				try {
					streamReader.close();
				} catch (IOException e) {
					System.err.println("Cannot close stream input reader! " + e);
				}
			}
			if (reader != null) {
				try {
					streamReader.close();
				} catch (IOException e) {
					System.err.println("Cannot close stream input reader! " + e);
				}
			}
		}
		//Return the output from the command - may be null if an error occured
		return output;
	}

	void getConnz() {
		URL url = null;
		try {
			url = new URL("http://localhost:8222/connz");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
			for (String line; (line = reader.readLine()) != null; ) {
				System.out.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}
	
	public static void setLogLevel(@SuppressWarnings("rawtypes") Class clazz, Level level){
		setLogLevel(clazz.getCanonicalName(), level);
	}
	
	public static void setLogLevel(String clazz, Level level){
		System.setProperty(ORG_SLF4J_SIMPLE_LOGGER_LOG + clazz, level.toString());
	}

	/**
	 * @return
	 */
	public static List<Integer> getData() {
		final List<Integer> data = Arrays.asList(new Integer[] {
				1,
				2,
				3,
				4,
				5,
				6
		});
		return data;
	}

	public static JavaRDD<Tuple2<String, Integer>> getKeyValueStream(JavaSparkContext sc, String subject1) {
		final List<Integer> data = getData();
		JavaRDD<Integer> rdd = sc.parallelize(data);
				
		JavaRDD<Tuple2<String, Integer>> stream = 
				rdd.map((Function<Integer, Tuple2<String, Integer>>) value -> {
									return new Tuple2<String, Integer>(subject1, value);
								});
		return stream;
	}

	public static JavaRDD<Integer> getJavaRDD(JavaSparkContext sc) {
		final List<Integer> data = getData();

		JavaRDD<Integer> rdd = sc.parallelize(data);
		return rdd;
	}

	/**
	 * @param data
	 * @param subject
	 * @param url
	 * @return
	 */
	public static StandardNatsSubscriber getStandardNatsSubscriber(final List<Integer> data, String subject, String url) {
		ExecutorService executor = Executors.newFixedThreadPool(1);
	
		final StandardNatsSubscriber ns = new StandardNatsSubscriber(url, subject + "_id", subject, data.size());
	
		// start the subscribers apps
		executor.execute(ns);
	
		// wait for subscribers to be ready.
		ns.waitUntilReady();
		return ns;
	}

	/**
	 * @param data
	 * @param subject
	 * @param clusterName
	 * @param clientName
	 * @param url
	 * @return
	 */
	public static NatsStreamingSubscriber<Integer> getNatsStreamingSubscriber(final List<Integer> data, String subject, String clusterName, String clientName, String url) {
		ExecutorService executor = Executors.newFixedThreadPool(1);

		NatsStreamingSubscriber<Integer> ns = new NatsStreamingSubscriber<Integer>(url, subject + "_id", subject, clusterName, clientName, data, Integer.class);

		// start the subscribers apps
		executor.execute(ns);

		// wait for subscribers to be ready.
		ns.waitUntilReady();
		return ns;
	}

	public static SparkConf newSparkConf() {
		SparkConf conf = new SparkConf()
			.setMaster(SPARK_MASTER)
//			.set("spark.executor.cores", UnitTestUtilities.getProperty("spark.executor.cores", "1"))
//			.set("spark.cores.max", UnitTestUtilities.getProperty("spark.cores.max", "2"))
			;

		//-					.set("spark.driver.host", "localhost"); // https://issues.apache.org/jira/browse/

		switch (TEST_MODE) {
		case cluster : 
			final File targetDir = new File("target");
			for (File file : targetDir.listFiles()) {
				if (file.getName().endsWith((".pom"))) {
					final String fileName = file.getName();
					final String jarFile = "target/" + fileName.substring(0, fileName.length() - 3) + "jar";
					LOGGER.info("{} shared with the Spark CLuster", jarFile);
					return conf.setJars(new String[] {jarFile});
				}
			}		

		default : return conf;
		}
	}
}