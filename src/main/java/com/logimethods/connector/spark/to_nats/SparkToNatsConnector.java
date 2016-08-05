/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.util.Collection;
import java.util.Properties;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Spark to NATS connector.
 * <p>
 * It provides a VoidFunction&lt;String&gt; method that can be called as follow:
 * <pre>rdd.foreach(SparkToNatsConnector.publishToNats( ... ));</pre>
 */
public abstract class SparkToNatsConnector<T> extends AbstractSparkToNatsConnector<T> {

	public static final String CLOSE_CONNECTION = "___Cl0seConnectION___";

	protected Properties properties;

	protected Collection<String> subjects;
	protected String natsURL;
	protected transient Integer sealedHashCode;
	
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
		super(null, null, (Collection<String>)null);
		logger.info("CREATE SparkToNatsConnector: " + this);
	}

	protected SparkToNatsConnector(String natsURL, Properties properties, String... subjects) {
		super(natsURL, properties, subjects);
		logger.info("CREATE SparkToNatsConnector {} with Properties '{}' and NATS Subjects '{}'.", this, properties, subjects);
	}

	protected SparkToNatsConnector(String natsURL, Properties properties, Collection<String> subjects) {
		super(natsURL, properties, subjects);
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
		return new SparkToStandardNatsConnectorImpl(null, null, null);
	}

	/**
	 * A VoidFunction&lt;String&gt; method that will publish the provided String into NATS through the defined subjects.
	 */
	protected VoidFunction<String> publishToNats = new VoidFunction<String>() {
		private static final long serialVersionUID = 1L;

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

	protected abstract Properties getEnrichedProperties();

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
			natsURL = getEnrichedProperties().getProperty(getsNatsUrlKey());
		}
		return natsURL;
	}

	protected abstract String getsNatsUrlKey();

	/**
	 * @return the logger
	 */
	protected Logger getLogger() {
		return logger;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((natsURL == null) ? 0 : natsURL.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((subjects == null) ? 0 : subjects.hashCode());
		return result;
	}

	/**
	 * @return the sealedHashCode
	 */
	protected Integer sealedHashCode() {
		if (sealedHashCode == null) {
			sealedHashCode = hashCode();
		}
		return sealedHashCode;
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
		if (natsURL == null) {
			if (other.natsURL != null)
				return false;
		} else if (!natsURL.equals(other.natsURL))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (publishToNats == null) {
			if (other.publishToNats != null)
				return false;
		} else if (!publishToNats.equals(other.publishToNats))
			return false;
		if (subjects == null) {
			if (other.subjects != null)
				return false;
		} else if (!subjects.equals(other.subjects))
			return false;
		return true;
	}
}
