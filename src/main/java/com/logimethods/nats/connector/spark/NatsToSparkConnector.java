/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
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
 * final JavaReceiverInputDStream&lt;String&gt; messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));
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

	protected Properties			properties = null;
	protected Collection<String>	subjects = null;
	protected String 				queue;

	protected NatsToSparkConnector(Properties properties, StorageLevel storageLevel, String... subjects) {
		super(storageLevel);
		this.properties = properties;
		this.subjects = Utilities.transformIntoAList(subjects);
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	protected NatsToSparkConnector(StorageLevel storageLevel, String... subjects) {
		super(storageLevel);
		this.subjects = Utilities.transformIntoAList(subjects);
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {} with Storage Level {} and NATS Subjects '{}'.", this, properties, subjects);
	}

	protected NatsToSparkConnector(Properties properties, StorageLevel storageLevel) {
		super(storageLevel);
		this.properties = properties;
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}' and Storage Level {}.", this, properties, storageLevel);
	}

	public NatsToSparkConnector(StorageLevel storageLevel) {
		super(storageLevel);
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {}.", this, properties, storageLevel);
	}

	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 * @param subjects The list of NATS subjects to publish to.
	 */
	public static NatsToSparkConnector receiveFromNats(Properties properties, StorageLevel storageLevel, String... subjects) {
		return new NatsToSparkConnector(properties, storageLevel, subjects);
	}

	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 * The settings of the NATS connection can be defined thanks to the System Properties.
	 *
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 * @param subjects The list of NATS subjects to publish to.
	 */
	public static NatsToSparkConnector receiveFromNats(StorageLevel storageLevel, String... subjects) {
		return new NatsToSparkConnector(storageLevel, subjects);
	}

	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 * The list of the NATS subjects (separated by ',') needs to be provided by the nats.io.connector.spark.subjects property.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 */
	public static NatsToSparkConnector receiveFromNats(Properties properties, StorageLevel storageLevel) {
		return new NatsToSparkConnector(properties, storageLevel);
	}

	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 * The settings of the NATS connection can be defined thanks to the System Properties.
	 *
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 */
	public static NatsToSparkConnector receiveFromNats(StorageLevel storageLevel) {
		return new NatsToSparkConnector(storageLevel);
	}

	protected void setQueue() {
		queue = "Q" + System.identityHashCode(this) ;
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

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception **/
	protected void receive() throws Exception {

		// Make connection and initialize streams			  
		final ConnectionFactory connectionFactory = new ConnectionFactory(getProperties());
		final Connection connection = connectionFactory.createConnection();
		logger.info("A NATS from '{}' to Spark Connection has been created for Subject '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, queue);
		
		for (String subject: getSubjects()) {
			final Subscription sub = connection.subscribe(subject, queue, new MessageHandler() {
				@Override
				public void onMessage(Message m) {
					String s = new String(m.getData());
					if (logger.isTraceEnabled()) {
						logger.trace("Received by {} on Subject '{}' sharing Queue '{}': {}.", NatsToSparkConnector.this, m.getSubject(), queue, s);
					}
					store(s);
				}
			});
			logger.info("Listening on {}.", subject);
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
				@Override
				public void run() {
					logger.info("Caught CTRL-C, shutting down gracefully...");
					try {
						sub.unsubscribe();
					} catch (IOException e) {
						logger.error("Problem while unsubscribing " + e.toString());
					}
					connection.close();
				}
			}));
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

