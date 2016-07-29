/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import java.util.Collection;
import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@SuppressWarnings("serial")
public abstract class NatsToSparkConnector extends Receiver<String> {

	static final Logger logger = LoggerFactory.getLogger(NatsToSparkConnector.class);
	
	protected Collection<String> subjects = null;
	protected Properties		 properties = null;

	public static final String NATS_SUBJECTS = "nats.io.connector.nats2spark.subjects";

	protected NatsToSparkConnector(StorageLevel storageLevel) {
		super(storageLevel);
	}

	protected NatsToSparkConnector(StorageLevel storageLevel, String... subjects) {
		super(storageLevel);
		this.subjects = Utilities.transformIntoAList(subjects);
	}
	
	public NatsToSparkConnector withSubjects(String... subjects) {
		this.subjects = Utilities.transformIntoAList(subjects);
		return this;
	}

	/* **************** STANDARD NATS **************** */
	
	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 * @param subjects The list of NATS subjects to publish to.
	 * @return a NATS to Spark Connector.
	 */
	@Deprecated
	public static NatsStandardToSparkConnectorImpl receiveFromNats(Properties properties, StorageLevel storageLevel, String... subjects) {
		return new NatsStandardToSparkConnectorImpl(properties, storageLevel, subjects);
	}

	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 * The settings of the NATS connection can be defined thanks to the System Properties.
	 *
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 * @param subjects The list of NATS subjects to publish to.
	 * @return a NATS to Spark Connector.
	 */
	@Deprecated
	public static NatsStandardToSparkConnectorImpl receiveFromNats(StorageLevel storageLevel, String... subjects) {
		return new NatsStandardToSparkConnectorImpl(storageLevel, subjects);
	}

	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 * The list of the NATS subjects (separated by ',') needs to be provided by the nats.io.connector.spark.subjects property.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 * @return a NATS to Spark Connector.
	 */
	@Deprecated
	public static NatsStandardToSparkConnectorImpl receiveFromNats(Properties properties, StorageLevel storageLevel) {
		return new NatsStandardToSparkConnectorImpl(properties, storageLevel);
	}

	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 * The settings of the NATS connection can be defined thanks to the System Properties.
	 *
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 * @return a NATS to Spark Connector.
	 */
	public static NatsStandardToSparkConnectorImpl receiveFromNats(StorageLevel storageLevel) {
		return new NatsStandardToSparkConnectorImpl(storageLevel);
	}

	/* **************** NATS STREAMING **************** */
	
	public static NatsStreamingToSparkConnectorImpl receiveFromNatsStreaming(StorageLevel storageLevel, String clusterID, String clientID, String... subjects) {
		return new NatsStreamingToSparkConnectorImpl(storageLevel, clusterID, clientID);
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
	protected abstract void receive() throws Exception;

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

