/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.Utilities;

/**
 * A Spark to NATS connector.
 * <p>
 * It provides a VoidFunction&lt;String&gt; method that can be called as follow:
 * <pre>rdd.foreach(SparkToNatsConnector.publishToNats( ... ));</pre>
 */
public abstract class SparkToNatsConnector<T> extends AbstractSparkToNatsConnector<T> {

	protected static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	protected Properties properties;

	protected Collection<String> subjects;
	protected String natsURL;
	protected Long connectionTimeout;
	protected transient ScheduledFuture<?> closingFuture;
	protected long internalId = Utilities.generateUniqueID(this);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected static final Logger logger = LoggerFactory.getLogger(SparkToNatsConnector.class);

	/**
	 * @param properties
	 * @param subjects
	 */
	protected SparkToNatsConnector() {
		super(null, null, null, (Collection<String>)null);
		logger.info("CREATE SparkToNatsConnector: " + this);
	}

	protected SparkToNatsConnector(String natsURL, Properties properties, Long connectionTimeout, String... subjects) {
		super(natsURL, properties, connectionTimeout, subjects);
		logger.info("CREATE SparkToNatsConnector {} with Properties '{}' and NATS Subjects '{}'.", this, properties, subjects);
	}

	protected SparkToNatsConnector(String natsURL, Properties properties, Long connectionTimeout, Collection<String> subjects) {
		super(natsURL, properties, connectionTimeout, subjects);
		logger.info("CREATE SparkToNatsConnector {} with Properties '{}' and NATS Subjects '{}'.", this, properties, subjects);
	}

	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the object from which the toString() will be published to NATS
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	public void publish(Object obj) throws Exception {
		final String str = obj.toString();
		publishToStr(str);
	}

	/**
	 */
	public static SparkToStandardNatsConnectorImpl newConnection() {
		return new SparkToStandardNatsConnectorImpl(null, null, null, null);
	}

	/**
	 * A VoidFunction&lt;String&gt; method that will publish the provided String into NATS through the defined subjects.
	 */
	protected VoidFunction<String> publishToNats = new VoidFunction<String>() {
		private static final long serialVersionUID = 2107780814969814070L;

		@Override
		public void call(String str) throws Exception {
			logger.trace("Publish to NATS: " + str);
			publishToStr(str);
		}
	};
	
	/**
	 * @param properties the properties to set
	 */
	protected void setProperties(Properties properties) {
		this.properties = properties;
	}

	protected abstract void publishToStr(String str) throws Exception;

	/**
	 * @param subjects the subjects to set
	 */
	protected void setSubjects(Collection<String> subjects) {
		this.subjects = subjects;
	}

	/**
	 * @param natsURL the natsURL to set
	 */
	protected void setNatsURL(String natsURL) {
		this.natsURL = natsURL;
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
	 * @return the natsURL
	 */
	protected String getNatsURL() {
		if (natsURL == null) {
			natsURL = getProperties().getProperty(getsNatsUrlKey());
		}
		return natsURL;
	}

	protected abstract String getsNatsUrlKey();

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
	 * @return the internalId
	 */
	public long getInternalId() {
		return internalId;
	}

	/**
	 * @return the logger
	 */
	protected Logger getLogger() {
		return logger;
	}

	/**
	 * 
	 */
	protected void resetClosingTimeout() {
		if (connectionTimeout != null) {
			logger.debug("At {}, READY to resetClosingTimeout({})", new Date().getTime(), this);
			synchronized(scheduler) {
				logger.debug("At {}, STARTING to resetClosingTimeout({})", new Date().getTime(), this);
				if ((closingFuture != null) && (closingFuture.getDelay(TimeUnit.NANOSECONDS) < connectionTimeout)) {
					closingFuture.cancel(false);
					closingFuture = null;
				}
				if (closingFuture == null) {
					closingFuture = scheduler.schedule(() -> closeConnection(), 2 * connectionTimeout, TimeUnit.NANOSECONDS);
					logger.debug("Will start at {}, STARTING to resetClosingTimeout({})", new Date().getTime() + closingFuture.getDelay(TimeUnit.MILLISECONDS), this);
				}
			}
		}
	}

	protected abstract void closeConnection();
	
	protected abstract void removeFromPool();
}
