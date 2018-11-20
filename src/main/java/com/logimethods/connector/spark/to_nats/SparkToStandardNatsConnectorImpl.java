/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static io.nats.client.Options.PROP_URL;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;

class SparkToStandardNatsConnectorImpl extends SparkToNatsConnector<SparkToStandardNatsConnectorImpl> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected static final Logger logger = LoggerFactory.getLogger(SparkToStandardNatsConnectorImpl.class);
	protected transient Connection connection;

	/**
	 * @param properties
	 * @param subjects
	 */
	protected SparkToStandardNatsConnectorImpl() {
		super();
	}
	
	/**
	 * @param properties
	 * @param subjects
	 * @param b 
	 */
	protected SparkToStandardNatsConnectorImpl(String natsURL, Properties properties, Long connectionTimeout, 
			Collection<String> subjects, boolean isStoredAsKeyValue) {
		super(natsURL, properties, connectionTimeout, subjects);
		setStoredAsKeyValue(isStoredAsKeyValue);
	}

	/**
	 * @param properties
	 * @param subjects
	 */
	protected SparkToStandardNatsConnectorImpl(String natsURL, Properties properties, Long connectionTimeout, 
			String... subjects) {
		super(natsURL, properties, connectionTimeout, subjects);
	}

	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the String that will be published to NATS.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	@Override
	protected void publishToNats(byte[] payload) throws Exception {
		resetClosingTimeout();
		
		final Message natsMessage = new Message();
	
		natsMessage.setData(payload, 0, payload.length);
	
		final Connection localConnection = getConnection();
		for (String subject : getDefinedSubjects()) {
			natsMessage.setSubject(subject);
			localConnection.publish(natsMessage);
	
			logger.trace("Send '{}' from Spark to NATS ({})", payload, subject);
		}
	}

	// TODO Check Javadoc
	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the String that will be published to NATS.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	@Override
	protected void publishToNats(String postSubject, byte[] payload) throws Exception {
		resetClosingTimeout();

		final Message natsMessage = new Message();		
		natsMessage.setData(payload, 0, payload.length);
	
		final Connection localConnection = getConnection();
		for (String preSubject : getDefinedSubjects()) {
			final String subject = combineSubjects(preSubject, postSubject);
			natsMessage.setSubject(subject);
			localConnection.publish(natsMessage);
	
			logger.trace("Send '{}' from Spark to NATS ({})", payload, subject);
		}
	}

	protected synchronized Connection getConnection() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		return connection;
	}
	
	protected Connection createConnection() throws IOException, TimeoutException, Exception {
		final Connection newConnection = 
				(getProperties() != null) ? Nats.connect(new Options.Builder(getProperties()).build()) :
					(getNatsURL() != null ) ? Nats.connect(getNatsURL()) :
						Nats.connect();

		logger.debug("A NATS Connection {} has been created for {}", newConnection, this);
		
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
		logger.debug("At {}, ready to close '{}' by {}", new Date().getTime(), connection, super.toString());
		removeFromPool();

		if (connection != null) {
			connection.close();
			logger.debug("{} has been CLOSED by {}", connection, super.toString());
			connection = null;
		}
	}
	
	@Override
	protected void removeFromPool() {
		SparkToStandardNatsConnectorPool.removeConnectorFromPool(this);
	}
	
	protected String getsNatsUrlKey() {
		return PROP_URL;
	}

	@Override
	protected int computeConnectionSignature() {
		return sparkToStandardNatsConnectionSignature(natsURL, properties, subjects, connectionTimeout);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SparkToStandardNatsConnectorImpl ["
				+ internalId + " / "
				+ super.toString() + " : "
				+ "connection=" + connection + ", "
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (subjects != null ? "subjects=" + subjects + ", " : "")
				+ (natsURL != null ? "natsURL=" + natsURL + ", " : "")
				+ "storedAsKeyValue=" + storedAsKeyValue + "]";
	}
}
