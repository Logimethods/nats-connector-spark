/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pool of SparkToNatsConnector(s).
 * @see <a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd">Design Patterns for using foreachRDD</a>
 * @see <a href="https://github.com/Logimethods/nats-connector-spark/blob/master/README.md">NATS / Spark Connectors README (on Github)</a>
 */
public abstract class SparkToNatsConnectorPool<T> extends AbstractSparkToNatsConnector<T> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5772600382175265781L;
	
	protected Properties				properties;
	protected Collection<String>		subjects;
	protected String 					natsURL;
	// TODO ? Map of connectorsPool by characteristic (hash code based on Streaming or not, natsurl, etc.)
	protected LinkedList<SparkToNatsConnector<?>> connectorsPool = new LinkedList<SparkToNatsConnector<?>>();

	static final Logger logger = LoggerFactory.getLogger(SparkToNatsConnectorPool.class);

	/**
	 * Create a pool of SparkToNatsConnector(s). 
	 * A connector can be obtained from that pool through getConnector() and released through returnConnector(SparkToNatsConnector connector).
	 * @see <a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd">Design Patterns for using foreachRDD</a>
	 */
	public SparkToNatsConnectorPool() {
		super();
		logger.debug("CREATE SparkToNatsConnectorPool: " + this);
	}

	/**
	 * Create a pool of SparkToNatsConnector(s).
	 * A connector can be obtained from that pool through getConnector() and released through returnConnector(SparkToNatsConnector connector).
	 * @param properties defines the properties of the connection to NATS.
	 * @param subjects defines the NATS subjects to which the messages will be pushed.
	 * @see <a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd">Design Patterns for using foreachRDD</a>
	 */
	public SparkToNatsConnectorPool(String natsURL, Properties properties, String... subjects) {
		super(natsURL, properties, subjects);
		logger.debug("CREATE SparkToNatsConnectorPool {} with Properties '{}' and NATS Subjects '{}'.", this, properties, subjects);
	}

	/**
	 * Create a pool of SparkToNatsConnector(s).
	 * A connector can be obtained from that pool through getConnector() and released through returnConnector(SparkToNatsConnector connector).
	 * @param properties defines the properties of the connection to NATS.
	 * @see <a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd">Design Patterns for using foreachRDD</a>
	 */
	public SparkToNatsConnectorPool(Properties properties) {
		super(null, properties, (Collection<String>)null);
		logger.debug("CREATE SparkToNatsConnectorPool {} with Properties '{}'.", this, properties);
	}

	/**
	 * Create a pool of SparkToNatsConnector(s).
	 * A connector can be obtained from that pool through getConnector() and released through returnConnector(SparkToNatsConnector connector).
	 * @param subjects defines the NATS subjects to which the messages will be pushed.
	 * @see <a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd">Design Patterns for using foreachRDD</a>
	 */
	public SparkToNatsConnectorPool(String... subjects) {
		super(null, null, subjects);
		logger.debug("CREATE SparkToNatsConnectorPool {} with NATS Subjects '{}'.", this, subjects);
	}
	
	public static SparkToStandardNatsConnectorPool newPool() {
		return new SparkToStandardNatsConnectorPool();
	}

	/**
	 * @return a SparkToNatsConnector from the Pool of Connectors (if not empty), otherwise create and return a new one.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	public abstract SparkToNatsConnector<?> getConnector() throws Exception;
	
	/**
	 * @param connector the SparkToNatsConnector to add to the Pool of Connectors.
	 */
	public void returnConnector(SparkToNatsConnector<?> connector) {
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
	 * @param natsURL the natsURL to set
	 */
	protected void setNatsURL(String natsURL) {
		this.natsURL = natsURL;
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
	 * @return the natsURL
	 */
	protected String getNatsURL() {
		return natsURL;
	}

	/**
	 * @return the logger
	 */
	protected Logger getLogger() {
		return logger;
	}
}
