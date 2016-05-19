/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Subscription;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link org.apache.spark.streaming.receiver.Receiver}&lt;String&gt;.
 * <p>
 * An usage of this class would look like this.
 * <pre>
 * JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
 * final JavaReceiverInputDStream&lt;String&gt; messages = ssc.receiverStream(
 *    new NatsToSparkConnector(null, 0, "Subject", "Group", StorageLevel.MEMORY_ONLY()));
 * </pre>
 * @see <a href="http://spark.apache.org/docs/1.6.1/streaming-custom-receivers.html">Spark Streaming Custom Receivers</a>
 */
public class NatsToSparkConnector extends Receiver<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public static final String NATS_SUBJECTS = "nats.io.connector.nats2spark.subjects";


	static final Logger logger = LoggerFactory.getLogger(NatsToSparkConnector.class);

	protected Properties			properties		  = null;
	protected Collection<String>	subjects;
/*	String host = null;
	int port = -1;	 
	String subject;
	String qgroup;
	String url;
*/			
	public NatsToSparkConnector(Properties properties, StorageLevel storageLevel, String... subjects) {
		super(storageLevel);
		this.properties = properties;
		this.subjects = Utilities.transformIntoAList(subjects);
		logger.debug("CREATE SparkToNatsConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);

/*		host = host_;
		port = port_;
		subject = _subject;
		qgroup = _qgroup;
		url = ConnectionFactory.DEFAULT_URL;
		if (host != null){
			url = url.replace("localhost", host);
		}
		if (port > 0){
			String strPort = Integer.toString(port);
			url = url.replace("4222", strPort);
		}*/
	}


	public NatsToSparkConnector(StorageLevel _storageLevel) {
		super(_storageLevel);
	}

	@Override
	public void onStart() {
		//Start the thread that receives data over a connection
		new Thread()  {
			@Override public void run() {
				try {
					receive();
				} catch (Exception e) {
					logger.error("Cannot start the connector: ", e);
				}
			}
		}.start();
	}

	@Override
	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself if CTRL-C is Caught.
	}

	/** Create a socket connection and receive data until receiver is stopped **/
	protected void receive() {

		try {
			// Make connection and initialize streams			  
			final ConnectionFactory connectionFactory = new ConnectionFactory(getProperties());
			final Connection connection = connectionFactory.createConnection();
			logger.info("A NATS Connection to '{}' has been created for {}", connection.getConnectedUrl(), this);
			
			for (String subject: getSubjects()) {
				final Subscription sub = connection.subscribe(subject, "group", m -> {
					String s = new String(m.getData());
					if (logger.isTraceEnabled()) {
						logger.trace("Received on {}: {}.", m.getSubject(), s);
					}
					store(s);
				});
				logger.info("Listening on {}.", subject);
				
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					logger.info("Caught CTRL-C, shutting down gracefully...");
					try {
						sub.unsubscribe();
					} catch (IOException e) {
						logger.error("Problem while unsubscribing " + e.toString());
					}
					connection.close();
				}));
			}
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage());
		}
	}

	protected Properties getProperties(){
		if (properties == null) {
			properties = new Properties(System.getProperties());
		}
		return properties;
	}

	protected Collection<String> getSubjects() throws Exception {
		if ((subjects ==  null) || (subjects.size() == 0)) {
			final String subjectsStr = getProperties().getProperty(NATS_SUBJECTS);
			if (subjectsStr == null) {
				throw new Exception("NatsToSparkConnector needs at least one NATS Subject.");
			}
			final String[] subjectsArray = subjectsStr.split(",");
			subjects = Utilities.transformIntoAList(subjectsArray);
			logger.debug("Subject provided by the Properties: '{}'", subjects);
		}
		return subjects;
	}    		
}

