/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import java.util.Collection;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.Message;

/**
 * A Spark to NATS connector.
 * <p>
 * It provides a VoidFunction&lt;String&gt; method that can be called as follow:
 * <pre>rdd.foreach(SparkToNatsConnector.publishToNats( ... ));</pre>
 */
public class SparkToNatsConnection {

	public static final String CLOSE_CONNECTION = "___Cl0seConnectION___";

	protected final AbstractSparkToNatsConnector connector;
	protected Connection connection = null;
	protected Collection<String> subjects = null;

	static final Logger logger = LoggerFactory.getLogger(SparkToNatsConnection.class);

	/**
	 * @param properties
	 * @param subjects
	 */
	protected SparkToNatsConnection(AbstractSparkToNatsConnector connector) {
		super();
		this.connector = connector;
		logger.debug("CREATE SparkToNatsConnection: " + this);
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

	protected Collection<String> getDefinedSubjects() throws Exception {
		if (subjects ==  null) {
			subjects = connector.getDefinedSubjects();
		}
		return subjects;		
	}

	protected Connection getDefinedConnection() throws Exception {
		if (connection ==  null) {
			connection = connector.createConnection();
		}
		return connection;		
	}
	
	/**
	 * @return the logger
	 */
	protected Logger getLogger() {
		return logger;
	}
}
