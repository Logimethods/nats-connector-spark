/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static io.nats.client.Constants.PROP_URL;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;

public class SparkToStandardNatsConnectorImpl extends SparkToNatsConnector<SparkToStandardNatsConnectorImpl> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected static final Logger logger = LoggerFactory.getLogger(SparkToStandardNatsConnectorImpl.class);
	protected Properties enrichedProperties;
	protected transient ConnectionFactory connectionFactory;
	protected transient Connection connection;

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStandardNatsConnectorImpl() {
		super();
	}
	
	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStandardNatsConnectorImpl(String natsURL, Properties properties, Long connectionTimeout, ConnectionFactory connectionFactory, Collection<String> subjects) {
		super(natsURL, properties, connectionTimeout, subjects);
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStandardNatsConnectorImpl(String natsURL, Properties properties, Long connectionTimeout, ConnectionFactory connectionFactory, String... subjects) {
		super(natsURL, properties, connectionTimeout, subjects);
		this.connectionFactory = connectionFactory;
	}

	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the object from which the toString() will be published to NATS
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	public VoidFunction<String> publishToNats() throws Exception {
		return publishToNats;
	}

	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the String that will be published to NATS.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	@Override
	protected void publishToStr(String str) throws Exception {
		resetClosingTimeout();
		
		logger.debug("publishToStr '{}' by {} through {}", str, super.toString(), connection);
		
		final Message natsMessage = new Message();
	
		final byte[] payload = str.getBytes();
		natsMessage.setData(payload, 0, payload.length);
	
		final Connection localConnection = getConnection();
		for (String subject : getDefinedSubjects()) {
			natsMessage.setSubject(subject);
			localConnection.publish(natsMessage);
	
			logger.trace("Send '{}' from Spark to NATS ({})", str, subject);
		}
	}

	protected synchronized Connection getConnection() throws Exception {
		if (connection == null) {
			connection = createConnection();
			logger.debug("A NATS Connection {} has been created for {}", connection, this);
		}
		return connection;
	}

	protected ConnectionFactory getConnectionFactory() throws Exception {
		if (connectionFactory == null) {
			if (getProperties() != null) {
				connectionFactory = new ConnectionFactory(getProperties());
			} else if (getNatsURL() != null ) {
				connectionFactory = new ConnectionFactory(getNatsURL());
			} else {
				connectionFactory = new ConnectionFactory();
			}
		}		
		return connectionFactory;
	}
	
	protected Connection createConnection() throws IOException, TimeoutException, Exception {
		final Connection newConnection = getConnectionFactory().createConnection();
		
		if (logger.isDebugEnabled()) {
			CONNECTIONS.add(newConnection);
			logger.debug(Arrays.toString(CONNECTIONS.toArray()));
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
			@Override
			public void run() {
				logger.debug("Caught CTRL-C, shutting down gracefully... " + this);
				newConnection.close();
			}
		}));
		
		return newConnection;
	}

	@Override
	protected synchronized void closeConnection() {
		logger.debug("Ready to close '{}' by {}", connection, super.toString());
		if (connection != null) {
			if (logger.isDebugEnabled()) {
				CONNECTIONS.remove(connection);
			}
			connection.close();
			logger.debug("{} has been CLOSED by {}", connection, super.toString());
			connection = null;
		}
	}
	
	protected String getsNatsUrlKey() {
		return PROP_URL;
	}

	@Override
	protected Properties getEnrichedProperties() {
		if ((enrichedProperties == null) && (getProperties() != null)) {
			enrichedProperties = getProperties();
			enrichedProperties.setProperty(getsNatsUrlKey(), getNatsURL());
		}
		return enrichedProperties;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SparkToStandardNatsConnectorImpl ["
				+ (connectionFactory != null ? "connectionFactory=" + connectionFactory + ", " : "")
				+ (connection != null ? "connection=" + connection + ", " : "")
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (subjects != null ? "subjects=" + subjects + ", " : "")
				+ (natsURL != null ? "natsURL=" + natsURL + ", " : "")
				+ (publishToNats != null ? "publishToNats=" + publishToNats : "") + "]";
	}

}
