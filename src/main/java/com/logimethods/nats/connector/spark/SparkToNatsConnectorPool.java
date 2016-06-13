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
import java.util.LinkedList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.ConnectionFactory;

/**
 * 
 * @author laugimethods
 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
 */
public class SparkToNatsConnectorPool extends AbstractSparkToNatsConnector implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5772600382175265781L;
	
	protected Properties				properties		  = null;
	protected Collection<String>		subjects;
	protected static ConnectionFactory 	connectionFactory = null;
	protected static LinkedList<SparkToNatsConnector> connectorsPool = new LinkedList<SparkToNatsConnector>();

	static final Logger logger = LoggerFactory.getLogger(SparkToNatsConnectorPool.class);

	/**
	 * @param properties
	 * @param subjects
	 */
	public SparkToNatsConnectorPool() {
		super();
		logger.debug("CREATE SparkToNatsConnectorPool: " + this);
	}

	public SparkToNatsConnectorPool(Properties properties, String... subjects) {
		super();
		this.properties = properties;
		this.subjects = Utilities.transformIntoAList(subjects);
		logger.debug("CREATE SparkToNatsConnectorPool {} with Properties '{}' and NATS Subjects '{}'.", this, properties, subjects);
	}

	/**
	 * @param properties
	 */
	public SparkToNatsConnectorPool(Properties properties) {
		super();
		this.properties = properties;
		logger.debug("CREATE SparkToNatsConnectorPool {} with Properties '{}'.", this, properties);
	}

	/**
	 * @param subjects
	 */
	public SparkToNatsConnectorPool(String... subjects) {
		super();
		this.subjects = Utilities.transformIntoAList(subjects);
		logger.debug("CREATE SparkToNatsConnectorPool {} with NATS Subjects '{}'.", this, subjects);
	}

	public SparkToNatsConnector getConnector() throws Exception {
		synchronized(connectorsPool) {
			if (connectorsPool.size() > 0) {
				return connectorsPool.pollFirst();
			}
		}
		
		return new SparkToNatsConnector(getDefinedProperties(), getDefinedSubjects(), getDefinedConnectionFactory());
	}
	
	public void returnConnector(SparkToNatsConnector connector) {
		synchronized(connectorsPool) {
			connectorsPool.add(connector);
		}
	}

	/**
	 * @return the properties
	 */
	protected Properties getProperties() {
		return properties;
	}

	/**
	 * @param properties the properties to set
	 */
	protected void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * @return the subjects
	 */
	protected Collection<String> getSubjects() {
		return subjects;
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
	 * @param connectionFactory the connectionFactory to set
	 */
	protected void setConnectionFactory(ConnectionFactory connectionFactory) {
		SparkToNatsConnectorPool.connectionFactory = connectionFactory;
	}

	/**
	 * @return the logger
	 */
	protected Logger getLogger() {
		return logger;
	}
}
