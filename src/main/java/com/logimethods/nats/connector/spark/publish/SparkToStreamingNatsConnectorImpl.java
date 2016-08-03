/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

public class SparkToStreamingNatsConnectorImpl extends SparkToNatsConnector<SparkToStreamingNatsConnectorImpl> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected final String clusterID = "test-cluster";
	protected String clientID;
	protected transient ConnectionFactory connectionFactory;
	protected transient Connection connection;

	/**
	 * 
	 */
	protected SparkToStreamingNatsConnectorImpl() {
		super();
	}

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStreamingNatsConnectorImpl(String natsURL, Properties properties, ConnectionFactory connectionFactory, Collection<String> subjects) {
		super(natsURL, properties, subjects);
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStreamingNatsConnectorImpl(String natsURL, Properties properties, ConnectionFactory connectionFactory, String... subjects) {
		super(natsURL, properties, subjects);
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @return the clusterID
	 */
	protected String getClusterID() {
		return clusterID;
	}

	/**
	 * @return the clientID
	 */
	protected String getClientID() {
		return "Client" + new Date().getTime();
	}

	@Override
	protected void publishToStr(String str) throws Exception {
		logger.debug("Received '{}' from Spark", str);

		if (CLOSE_CONNECTION.equals(str)) {
			closeConnection();
			return;
		}
		
		final byte[] payload = str.getBytes();
		final Connection localConnection = getConnection();
		for (String subject : getDefinedSubjects()) {
			localConnection.publish(subject, payload);
	
			logger.trace("Publish '{}' from Spark to NATS STREAMING ({})", str, subject);
		}
	}

	protected synchronized Connection getConnection() throws Exception {
		if (connection == null) {
			connection = createConnection();
			getLogger().debug("A NATS Connection {} has been created for {}", connection, this);
		}
		return connection;
	}

	protected ConnectionFactory getConnectionFactory() throws Exception {
		if (connectionFactory == null) {
			connectionFactory = new ConnectionFactory(getClusterID(), getClientID());
			connectionFactory.setNatsUrl(getNatsURL());
		}		
		return connectionFactory;
	}
	
	protected Connection createConnection() throws IOException, TimeoutException, Exception {
		return getConnectionFactory().createConnection();
	}

	public synchronized void closeConnection() throws IOException, TimeoutException {
		if (connection != null) {
			connection.close();
			connection = null;
		}
	}

}
