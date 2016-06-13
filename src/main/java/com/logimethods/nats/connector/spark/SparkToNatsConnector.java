/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;

/**
 * A Spark to NATS connector.
 * <p>
 * It provides a VoidFunction&lt;String&gt; method that can be called as follow:
 * <pre>rdd.foreach(SparkToNatsConnector.publishToNats( ... ));</pre>
 */
public class SparkToNatsConnector extends AbstractSparkToNatsConnector implements Serializable {

	public static final String CLOSE_CONNECTION = "___Cl0seConnectION___";

	protected ConnectionFactory connectionFactory = null;
	protected Connection connection = null;
	protected Properties properties = null;
	protected Collection<String> subjects;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(SparkToNatsConnector.class);

	/**
	 * @param properties
	 * @param subjects
	 */
	protected SparkToNatsConnector() {
		super();
		logger.debug("CREATE SparkToNatsConnector: " + this);
	}

	/**
	 * @param connectionFactory
	 * @param properties
	 * @param subjects
	 */
	protected SparkToNatsConnector(Properties properties, Collection<String> subjects, ConnectionFactory connectionFactory) {
		super();
		this.connectionFactory = connectionFactory;
		this.properties = properties;
		this.subjects = subjects;
	}

	protected SparkToNatsConnector(Properties properties, String... subjects) {
		super();
		this.properties = properties;
		this.subjects = Utilities.transformIntoAList(subjects);
		logger.debug("CREATE SparkToNatsConnector {} with Properties '{}' and NATS Subjects '{}'.", this, properties, subjects);
	}

	/**
	 * @param properties
	 */
	protected SparkToNatsConnector(Properties properties) {
		super();
		this.properties = properties;
		logger.debug("CREATE SparkToNatsConnector {} with Properties '{}'.", this, properties);
	}

	/**
	 * @param subjects
	 */
	protected SparkToNatsConnector(String... subjects) {
		super();
		this.subjects = Utilities.transformIntoAList(subjects);
		logger.debug("CREATE SparkToNatsConnector {} with NATS Subjects '{}'.", this, subjects);
	}

	/**
	 * Will publish the Strings provided (by Spark) into NATS.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 * @param subjects The list of NATS subjects to publish to.
	 */
	public static VoidFunction<String> publishToNats(Properties properties, String... subjects) {
		return new SparkToNatsConnector(properties, subjects).publishToNats;
	}

	/**
	 * Will publish the Strings provided (by Spark) into NATS.
	 * The list of the NATS subjects (separated by ',') needs to be provided by the nats.io.connector.spark.subjects property.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 */
	public static VoidFunction<String> publishToNats(Properties properties) {
		return new SparkToNatsConnector(properties).publishToNats;
	}

	/**
	 * Will publish the Strings provided (by Spark) into NATS.
	 * The settings of the NATS connection can be defined thanks to the System Properties.
	 *
	 * @param subjects The list of NATS subjects to publish to.
	 */
	public static VoidFunction<String> publishToNats(String... subjects) {
		return new SparkToNatsConnector(subjects).publishToNats;
	}

	public synchronized void closeConnection() {
		if (connection != null) {
			connection.close();
			connection = null;
		}
	}

	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @throws Exception 
	 */
	public void publishToNats(Object obj) throws Exception {
		String str = obj.toString();
		publishToNats(str);
	}

	protected synchronized Connection getDefinedConnection() throws Exception {
		if (getConnection() == null) {
			setConnection(createConnection());
			getLogger().debug("A NATS Connection {} has been created for {}", getConnection(), this);
		}
		return getConnection();
	}

	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @throws Exception 
	 */
	protected void publishToNatsStr(String str) throws Exception {
		if (CLOSE_CONNECTION.equals(str)) {
			closeConnection();
			return;
		}
		
		final Message natsMessage = new Message();

		final byte[] payload = str.getBytes();
		natsMessage.setData(payload, 0, payload.length);

		final Connection localConnection = getDefinedConnection();
		for (String subject : getDefinedSubjects()) {
			natsMessage.setSubject(subject);
			localConnection.publish(natsMessage);

			logger.trace("Send '{}' from Spark to NATS ({})", str, subject);
		}
	}

	/**
	 * A VoidFunction&lt;String&gt; method that will publish the provided String into NATS through the defined subjects.
	 */
	protected VoidFunction<String> publishToNats = new VoidFunction<String>() {
		private static final long serialVersionUID = 1L;

		@Override
		public void call(String str) throws Exception {
			publishToNatsStr(str);
		}
	};
	
	/**
	 * @param connectionFactory the connectionFactory to set
	 */
	protected void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param connection the connection to set
	 */
	protected void setConnection(Connection connection) {
		this.connection = connection;
	}

	/**
	 * @param properties the properties to set
	 */
	protected void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * @param subjects the subjects to set
	 */
	protected void setSubjects(Collection<String> subjects) {
		this.subjects = subjects;
	}

	/**
	 * @return the connectionFactory
	 */
	protected ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	/**
	 * @return the connection
	 */
	protected Connection getConnection() {
		return connection;
	}

	/**
	 * @return the properties
	 */
	protected Properties getProperties() {
		return properties;
	}

	/**
	 * @return the subjects
	 */
	protected Collection<String> getSubjects() {
		return subjects;
	}

	/**
	 * @return the logger
	 */
	protected Logger getLogger() {
		return logger;
	}
}
