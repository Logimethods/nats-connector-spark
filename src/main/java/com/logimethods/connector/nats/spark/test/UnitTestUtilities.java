/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;

public class UnitTestUtilities {

	private static final String ORG_SLF4J_SIMPLE_LOGGER_LOG = "org.slf4j.simpleLogger.log.";
	static NATSServer defaultServer = null;
	public static final int NATS_SERVER_PORT = 4221;
	public static final String NATS_SERVER_URL = "nats://localhost:"+NATS_SERVER_PORT;

	public static final int STANServerPORT = 4223;
	public static final String STAN_URL = "nats://localhost:" + STANServerPORT;	
	public static final String CLUSTER_ID = "test-cluster";


	Process authServerProcess = null;

	public static synchronized void startDefaultServer() {
		if (defaultServer == null) {
			defaultServer = new NATSServer(NATS_SERVER_PORT);
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
        STANServer srv = new STANServer(clusterID, STANServerPORT, debug);
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
	public static List<String> getData() {
		final List<String> data = Arrays.asList(new String[] {
				"1",
				"2",
				"3",
				"4",
				"5",
				"6"
		});
		return data;
	}

	/**
	 * @param data
	 * @return
	 */
	public static StandardNatsSubscriber getStandardNatsSubscriber(final List<String> data, String subject, String url) {
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
	 * @return
	 */
	public static NatsStreamingSubscriber getNatsStreamingSubscriber(final List<String> data, String subject, String clusterName, String clientName, String url) {
		ExecutorService executor = Executors.newFixedThreadPool(1);

		NatsStreamingSubscriber ns = new NatsStreamingSubscriber(url, subject + "_id", subject, clusterName, clientName, data.size());

		// start the subscribers apps
		executor.execute(ns);

		// wait for subscribers to be ready.
		ns.waitUntilReady();
		return ns;
	}
}