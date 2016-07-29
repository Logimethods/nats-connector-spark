/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;

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
public class NatsStreamingToSparkConnectorImpl extends NatsToSparkConnector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(NatsStreamingToSparkConnectorImpl.class);

	protected String clusterID, clientID;

	protected NatsStreamingToSparkConnectorImpl(StorageLevel storageLevel, String clusterID, String clientID, String... subjects) {
		super(storageLevel, subjects);
		this.clusterID = clusterID;
		this.clientID = clientID;
//		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception **/
	protected void receive() throws Exception {

		// Make connection and initialize streams			  
		final ConnectionFactory connectionFactory = new ConnectionFactory(clusterID, clientID);
		final Connection connection = connectionFactory.createConnection();
//		logger.info("A NATS from '{}' to Spark Connection has been created for '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, queue);
		
		for (String subject: getSubjects()) {
			final Subscription sub = connection.subscribe(subject, new MessageHandler() {
				@Override
				public void onMessage(Message m) {
					String s = new String(m.getData());
					if (logger.isTraceEnabled()) {
						logger.trace("Received by {} on Subject '{}': {}.", NatsStreamingToSparkConnectorImpl.this, m.getSubject(), s);
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
					} catch (IOException | TimeoutException e) {
						logger.error("Problem while unsubscribing " + e.toString());
					}
					try {
						connection.close();
					} catch (IOException | TimeoutException e) {
						logger.error("Problem while unsubscribing " + e.toString());
					}
				}
			}));
		}
	}
}

