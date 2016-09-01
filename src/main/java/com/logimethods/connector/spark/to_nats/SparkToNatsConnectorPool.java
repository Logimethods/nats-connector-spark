/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Constants.PROP_URL;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.Utilities;

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
	protected Long 						connectionTimeout;
//	protected /*transient*/ Integer 	connectorHashCode;
	protected static final HashMap<Integer, LinkedList<SparkToNatsConnector<?>>> connectorsPoolMap = new HashMap<Integer, LinkedList<SparkToNatsConnector<?>>>();

	static final Logger logger = LoggerFactory.getLogger(SparkToNatsConnectorPool.class);

	/**
	 * Create a pool of SparkToNatsConnector(s).
	 * A connector can be obtained from that pool through getConnector() and released through returnConnector(SparkToNatsConnector connector).
	 * @param subjects defines the NATS subjects to which the messages will be pushed.
	 * @see <a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd">Design Patterns for using foreachRDD</a>
	 */
	protected SparkToNatsConnectorPool(String... subjects) {
		super(null, null, null, subjects);
		logger.debug("CREATE SparkToNatsConnectorPool {} with NATS Subjects '{}'.", this, subjects);
	}
	
	public static SparkToStandardNatsConnectorPool newPool() {
		return new SparkToStandardNatsConnectorPool();
	}
	
	public static SparkToNatsStreamingConnectorPool newStreamingPool(String clusterID) {
		return new SparkToNatsStreamingConnectorPool(clusterID);
	}

	/**
	 * @return a SparkToNatsConnector from the Pool of Connectors (if not empty), otherwise create and return a new one.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	public SparkToNatsConnector<?> getConnector() throws Exception {
		synchronized(connectorsPoolMap) {
			logger.debug("{}: getConnector() for '{}' ConnectionSignature", super.toString(), getConnectionSignature());
//			if (connectorHashCode != null) {
				final LinkedList<SparkToNatsConnector<?>> connectorsPool = connectorsPoolMap.get(getConnectionSignature());
				if ((connectorsPool != null) && (connectorsPool.size() > 0)) {
					logger.debug("ConnectorsPool for {} of size {}", getConnectionSignature(), connectorsPool.size());
					final SparkToNatsConnector<?> connector = connectorsPool.pollFirst();
///					connector.poolList = null;
					return connector;
				} 
			}			
			SparkToNatsConnector<?> newConnector = newSparkToNatsConnector();
//			connectorHashCode = newConnector.sealedHashCode();
			logger.debug("New SparkToNatsConnector<?> {} created with ConnectionSignature {}", newConnector, getConnectionSignature());
			return newConnector;
//		}
	}

	/**
	 * @return
	 * @throws Exception
	 */
	protected abstract SparkToNatsConnector<?> newSparkToNatsConnector() throws Exception;
	
	/**
	 * @param connector the SparkToNatsConnector to add to the Pool of Connectors.
	 */
	public void returnConnector(SparkToNatsConnector<?> connector) {
		synchronized(connectorsPoolMap) {
			logger.debug("{}: Returning {} to pool", super.toString(), connector);
//			if (connector.hasANotNullConnection()) {
				final int connectionSignature = connector.getConnectionSignature();
				LinkedList<SparkToNatsConnector<?>> connectorsPoolList = connectorsPoolMap.get(connectionSignature);
				if (connectorsPoolList == null) {
					connectorsPoolList = new LinkedList<SparkToNatsConnector<?>>();
					connectorsPoolMap.put(connectionSignature, connectorsPoolList);
				}
////				connector.poolList = connectorsPoolList;
				connectorsPoolList.add(connector);
//			} else {
//				logger.debug("{} has a NULL connection, therefore will not be returned to a pool", connector);
//			}
		}
	}

	protected static void removeConnectorFromPool(SparkToNatsConnector<?> connector) {
		logger.debug("Removing {} from pool", connector);
		synchronized(connectorsPoolMap) {
			final int hashCode = connector.getConnectionSignature();
			LinkedList<SparkToNatsConnector<?>> connectorsPool = connectorsPoolMap.get(hashCode);
			if (connectorsPool != null) {
				connectorsPool.removeFirst(); // All connectors sharing the same ConnectionSignature are equivalent
				if (connectorsPool.size() == 0) { // No more connectors sharing that hashCode
					connectorsPoolMap.remove(hashCode);
				}			
			}
		}
	}
	
	/**
	 * @param rdd
	 * @see http://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
	 */
	@SuppressWarnings("deprecation")
	public void publishToNats(final JavaDStream<String> rdd) {
		rdd.foreachRDD((Function<JavaRDD<String>, Void>) rdd1 -> {
			logger.trace("rdd.foreachRDD");
			rdd1.foreachPartition(strings -> {
				logger.trace("rdd1.foreachPartition");
				final SparkToNatsConnector<?> connector = getConnector();
				while(strings.hasNext()) {
					final String str = strings.next();
					logger.trace("Will publish {}", str);
					connector.publish(str);
				}
				returnConnector(connector);  // return to the pool for future reuse
			});
			return null;
		});
	}

	public static long poolSize() {
		int size = 0;
		for (List<SparkToNatsConnector<?>> poolList: connectorsPoolMap.values()){
			size += poolList.size();
		}
		return size;
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
		if (properties != null) {
			if (properties.containsKey(PROP_SUBJECTS)) {
				setSubjects(Utilities.extractCollection(properties.getProperty(PROP_SUBJECTS)));
			}
			if (properties.containsKey(PROP_URL)) {
				setNatsURL(properties.getProperty(PROP_URL));
			}
		}
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

	/**
	 * @return the connectionTimeout
	 */
	@Override
	protected Long getConnectionTimeout() {
		return connectionTimeout;
	}

	/**
	 * @param connectionTimeout the connectionTimeout to set
	 */
	@Override
	protected void setConnectionTimeout(Long connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return sparkToStandardNatsConnectionSignature(natsURL, properties, subjects, connectionTimeout);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof SparkToNatsConnector))
			return false;
		SparkToNatsConnector<?> other = (SparkToNatsConnector<?>) obj;
		return (this.sealedHashCode() == other.sealedHashCode());
	}
}
