/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static com.logimethods.connector.nats_spark.NatsSparkUtilities.encodeData;
import static io.nats.client.Constants.PROP_URL;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import scala.Tuple2;

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
	protected boolean 					storedAsKeyValue = false;
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
		final int localConnectionSignature = getConnectionSignature();
		logger.debug("getConnector() for '{}' ConnectionSignature", localConnectionSignature);
		synchronized(connectorsPoolMap) {
			final LinkedList<SparkToNatsConnector<?>> connectorsPool = connectorsPoolMap.get(localConnectionSignature);
			if ((connectorsPool != null) && (connectorsPool.size() > 0)) {
				logger.debug("ConnectorsPool for {} of size {}", localConnectionSignature, connectorsPool.size());
				final SparkToNatsConnector<?> connector = connectorsPool.pollFirst();
				return connector;
			} 
		}			
		SparkToNatsConnector<?> newConnector = newSparkToNatsConnector();
		newConnector.setConnectionSignature(localConnectionSignature);
		logger.debug("New SparkToNatsConnector<?> {} created with ConnectionSignature {}", newConnector, localConnectionSignature);
		return newConnector;
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
		logger.debug("Returning {} to pool", connector);
		synchronized(connectorsPoolMap) {
			final int connectionSignature = connector.getConnectionSignature();
			LinkedList<SparkToNatsConnector<?>> connectorsPoolList = connectorsPoolMap.get(connectionSignature);
			if (connectorsPoolList == null) {
				connectorsPoolList = new LinkedList<SparkToNatsConnector<?>>();
				connectorsPoolMap.put(connectionSignature, connectorsPoolList);
			}
			connectorsPoolList.add(connector);
		}
	}

	protected static void removeConnectorFromPool(SparkToNatsConnector<?> connector) {
		logger.debug("Removing {} from pool", connector);
		synchronized(connectorsPoolMap) {
			final int connectionSignature = connector.getConnectionSignature();
			LinkedList<SparkToNatsConnector<?>> connectorsPool = connectorsPoolMap.get(connectionSignature);
			if (connectorsPool != null) {
				connectorsPool.removeFirst(); // All connectors sharing the same ConnectionSignature are equivalent
				if (connectorsPool.size() == 0) { // No more connectors sharing that hashCode
					connectorsPoolMap.remove(connectionSignature);
				}			
			}
		}
	}
	
	/**
	 * @param rdd
	 */
	public <V extends Object> void publishToNats(final JavaDStream<V> stream) {
		publishToNats(stream, (Function<V, byte[]> & Serializable) obj -> encodeData(obj));
	}
	
	/**
	 * @param rdd
	 */
	public <V extends Object> void publishToNats(final JavaDStream<V> stream, final Function<V, byte[]> dataEncoder) {
		logger.trace("publishToNats(JavaDStream<String> stream)");
		stream.foreachRDD((VoidFunction<JavaRDD<V>>) rdd -> {
			logger.trace("stream.foreachRDD");
			rdd.foreachPartitionAsync(objects -> {
				logger.trace("rdd.foreachPartition");
				final SparkToNatsConnector<?> connector = getConnector();
				while(objects.hasNext()) {
					final V obj = objects.next();
					logger.trace("Will publish {}", obj);
					connector.publishToNats(dataEncoder.apply(obj));
				}
				returnConnector(connector);  // return to the pool for future reuse
			});
		});
	}
	
	/**
	 * @param rdd
	 */
	public <K extends Object, V extends Object> void publishToNatsAsKeyValue(final JavaPairDStream<K, V> stream) {
		publishToNatsAsKeyValue(stream, (Function<V, byte[]> & Serializable) obj -> encodeData(obj));
	}
	
	/**
	 * @param rdd
	 */
	public <K extends Object, V extends Object> void publishToNatsAsKeyValue(final JavaPairDStream<K, V> stream, final Function<V, byte[]> dataEncoder) {
		logger.trace("publishToNats(JavaPairDStream<String, String> stream)");
		setStoredAsKeyValue(true);
		
		stream.foreachRDD((VoidFunction<JavaPairRDD<K, V>>) rdd -> {
			logger.trace("stream.foreachRDD");
			rdd.foreachPartitionAsync((VoidFunction<Iterator<Tuple2<K,V>>>) tuples -> {
				logger.trace("rdd.foreachPartition");
				final SparkToNatsConnector<?> connector = getConnector();
				while(tuples.hasNext()) {
					final Tuple2<K,V> tuple = tuples.next();
					logger.trace("Will publish {}", tuple);
					connector.publishToNats(tuple._1.toString(), dataEncoder.apply(tuple._2));
				}
				returnConnector(connector);  // return to the pool for future reuse
			});
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
				setSubjects(NatsSparkUtilities.extractCollection(properties.getProperty(PROP_SUBJECTS)));
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

	/**
	 * @return the storedAsKeyValue
	 */
	protected boolean isStoredAsKeyValue() {
		logger.trace("isStoredAsKeyValue() -> {}", storedAsKeyValue);
		return storedAsKeyValue;
	}

	/**
	 * @param storedAsKeyValue the storedAsKeyValue to set
	 */
	protected void setStoredAsKeyValue(boolean storedAsKeyValue) {
		logger.trace("setStoredAsKeyValue({})", storedAsKeyValue);
		this.storedAsKeyValue = storedAsKeyValue;
	}
}
