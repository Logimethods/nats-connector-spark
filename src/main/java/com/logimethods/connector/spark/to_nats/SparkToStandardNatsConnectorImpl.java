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
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.VoidFunction;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;

public class SparkToStandardNatsConnectorImpl extends SparkToNatsConnector<SparkToStandardNatsConnectorImpl> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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
	protected SparkToStandardNatsConnectorImpl(String natsURL, Properties properties, ConnectionFactory connectionFactory, Collection<String> subjects) {
		super(natsURL, properties, subjects);
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStandardNatsConnectorImpl(String natsURL, Properties properties, ConnectionFactory connectionFactory, String... subjects) {
		super(natsURL, properties, subjects);
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
	protected void publishToStr(String str) throws Exception {
		if (CLOSE_CONNECTION.equals(str)) {
			closeConnection();
			return;
		}
		
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
			getLogger().debug("A NATS Connection {} has been created for {}", connection, this);
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
		return getConnectionFactory().createConnection();
	}

	public synchronized void closeConnection() {
		if (connection != null) {
			connection.close();
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
