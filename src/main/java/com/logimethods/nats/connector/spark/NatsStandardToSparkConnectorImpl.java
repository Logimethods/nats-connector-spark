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

import org.apache.spark.storage.StorageLevel;
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
 * @see <a href="http://spark.apache.org/docs/1.6.2/streaming-custom-receivers.html">Spark Streaming Custom Receivers</a>
 */
public class NatsStandardToSparkConnectorImpl extends NatsToSparkConnector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(NatsStandardToSparkConnectorImpl.class);

	protected String 				queue;

	protected NatsStandardToSparkConnectorImpl(Properties properties, StorageLevel storageLevel, String... subjects) {
		super(storageLevel, subjects);
		this.properties = properties;
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	protected NatsStandardToSparkConnectorImpl(StorageLevel storageLevel, String... subjects) {
		super(storageLevel, subjects);
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {} with Storage Level {} and NATS Subjects '{}'.", this, properties, subjects);
	}

	protected NatsStandardToSparkConnectorImpl(Properties properties, StorageLevel storageLevel) {
		super(storageLevel);
		this.properties = properties;
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}' and Storage Level {}.", this, properties, storageLevel);
	}

	public NatsStandardToSparkConnectorImpl(StorageLevel storageLevel) {
		super(storageLevel);
		setQueue();
		logger.debug("CREATE NatsToSparkConnector {}.", this, properties, storageLevel);
	}

	protected void setQueue() {
		queue = "Q" + System.identityHashCode(this) ;
	}

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception **/
	protected void receive() throws Exception {

		// Make connection and initialize streams			  
		final ConnectionFactory connectionFactory = new ConnectionFactory(getProperties());
		final Connection connection = connectionFactory.createConnection();
		logger.info("A NATS from '{}' to Spark Connection has been created for '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, queue);
		
		for (String subject: getSubjects()) {
			final Subscription sub = connection.subscribe(subject, queue, new MessageHandler() {
				@Override
				public void onMessage(Message m) {
					String s = new String(m.getData());
					if (logger.isTraceEnabled()) {
						logger.trace("Received by {} on Subject '{}' sharing Queue '{}': {}.", NatsStandardToSparkConnectorImpl.this, m.getSubject(), queue, s);
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

