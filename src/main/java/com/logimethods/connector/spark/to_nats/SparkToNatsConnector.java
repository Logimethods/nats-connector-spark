/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import scala.Tuple2;

/**
 * A Spark to NATS connector.
 * <p>
 * It provides a VoidFunction&lt;String&gt; method that can be called as follow:
 * <pre>rdd.foreach(SparkToNatsConnector.publishToNats( ... ));</pre>
 */
public abstract class SparkToNatsConnector<T> extends AbstractSparkToNatsConnector<T> {

	protected static final String SUBJECT_PATTERN_SEPARATOR = "=>";

	protected static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	protected Properties properties;

	protected Collection<String> subjects;
	protected String natsURL;
	protected Long connectionTimeout;
	protected transient ScheduledFuture<?> closingFuture;
	protected long internalId = NatsSparkUtilities.generateUniqueID(this);
	protected boolean storedAsKeyValue = false;
	
	protected static final Map<String, Tuple2<Pattern, String>> subjectPatternMap = new HashMap<String, Tuple2<Pattern, String>>();
	
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
	 */
	public static SparkToStandardNatsConnectorImpl newConnection() {
		return new SparkToStandardNatsConnectorImpl(null, null, null, null);
	}

	/**
	 * A VoidFunction&lt;String&gt; method that will publish the provided String into NATS through the defined subjects.
	 */
	protected VoidFunction<?> publishToNats = new VoidFunction<Object>() {
		private static final long serialVersionUID = 3445253313354486580L;

		@Override
		public void call(Object str) throws Exception {
			logger.trace("Publish to NATS: " + str);
			publishToStr(str.toString());
		}
	};

	/**
	 * A VoidFunction&lt;String&gt; method that will publish the provided String into NATS through the defined subjects.
	 */
	protected VoidFunction<Tuple2<?,?>> publishKeyValueToNats = new VoidFunction<Tuple2<?,?>>() {
		private static final long serialVersionUID = -3056486640490904222L;

		@Override
		public void call(Tuple2<?,?> tuple) throws Exception {
			logger.trace("Publish to NATS: " + tuple);
			publishToStr(tuple._1.toString(), tuple._2.toString());
		}
	};
	
	/**
	 * @param properties the properties to set
	 */
	protected void setProperties(Properties properties) {
		this.properties = properties;
	}


	// TODO Check JavaDoc
	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the object from which the toString() will be published to NATS
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	protected void publish(Object obj) throws Exception {
		if (storedAsKeyValue) {
			final Tuple2<?, ?> tuple = (Tuple2<?, ?>) obj;
			publishToStr(tuple._1.toString(), tuple._2.toString());
		} else {
			publishToStr(obj.toString());
		}
	}

	// TODO Check JavaDoc
	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the object from which the toString() will be published to NATS
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	protected abstract void publishToStr(String str) throws Exception;

	// TODO Check JavaDoc
	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the object from which the toString() will be published to NATS
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	protected abstract void publishToStr(String subject, String message) throws Exception;

	protected static String combineSubjects(String preSubject, String postSubject) {
		if (preSubject.contains(SUBJECT_PATTERN_SEPARATOR)) {
			Pattern pattern;
			String replacement;
			if (subjectPatternMap.containsKey(preSubject)) {
				final Tuple2<Pattern, String> tuple = subjectPatternMap.get(preSubject);
				pattern = tuple._1;
				replacement = tuple._2;
			} else {
				// http://www.vogella.com/tutorials/JavaRegularExpressions/article.html
				final int pos = preSubject.indexOf(SUBJECT_PATTERN_SEPARATOR);
	
				final String patternStr = preSubject.substring(0, pos).trim().replace(".", "\\.").replace("*", "[^\\.]*");		
				logger.trace(patternStr);
				pattern = Pattern.compile(patternStr);
				
				replacement = preSubject.substring(pos+SUBJECT_PATTERN_SEPARATOR.length()).trim();
				logger.trace(replacement);	
				
				subjectPatternMap.put(preSubject, new Tuple2<Pattern, String>(pattern, replacement));
			}
			return pattern.matcher(postSubject).replaceFirst(replacement);
		} else {
			return preSubject + postSubject;
		}
	}

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
	 * @return the storedAsKeyValue
	 */
	protected boolean isStoredAsKeyValue() {
		return storedAsKeyValue;
	}

	/**
	 * @param storedAsKeyValue the storedAsKeyValue to set
	 */
	protected void setStoredAsKeyValue(boolean storedAsKeyValue) {
		this.storedAsKeyValue = storedAsKeyValue;
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
