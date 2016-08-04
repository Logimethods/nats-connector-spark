/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.publish;

import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.Utilities;

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
	 * @param properties
	 */
	protected SparkToNatsConnector(String natsURL, Properties properties) {
		super(natsURL, properties, (Collection<String>)null);
		logger.info("CREATE SparkToNatsConnector {} with Properties '{}'.", this, properties);
	}

	/**
	 * @param subjects
	 */
	protected SparkToNatsConnector(String natsURL, String... subjects) {
		super(natsURL, null, subjects);
		logger.info("CREATE SparkToNatsConnector {} with NATS Subjects '{}'.", this, subjects);
	}

	/**
	 * Will publish the Strings provided (by Spark) into NATS.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 * @param subjects The list of NATS subjects to publish to.
	 * @return a VoidFunction&lt;String&gt;, backed by a SparkToNatsConnector, that can be called to publish messages to NATS.
	 */
	@Deprecated
	public static VoidFunction<String> publishToNats(String natsURL, Properties properties, String... subjects) {
		return new SparkToStandardNatsConnectorImpl(natsURL, properties, null, subjects).publishToNats;
	}

	/**
	 * Will publish the Strings provided (by Spark) into NATS.
	 * The list of the NATS subjects (separated by ',') needs to be provided by the nats.io.connector.spark.subjects property.
	 *
	 * @param properties Defines the properties of the connection to NATS.
	 * @return a VoidFunction&lt;String&gt;, backed by a SparkToNatsConnector, that can be called to publish messages to NATS.
	 */
	@Deprecated
	public static VoidFunction<String> publishToNats(String natsURL, Properties properties) {
		return new SparkToStandardNatsConnectorImpl(natsURL, properties, null).publishToNats;
	}

	/**
	 * Will publish the Strings provided (by Spark) into NATS.
	 * The settings of the NATS connection can be defined thanks to the System Properties.
	 *
	 * @param subjects The list of NATS subjects to publish to.
	 * @return a VoidFunction&lt;String&gt;, backed by a SparkToNatsConnector, that can be called to publish messages to NATS.
	 */
	@Deprecated
	public static VoidFunction<String> publishToNats(String natsURL, String... subjects) {
		return new SparkToStandardNatsConnectorImpl(natsURL, null, null, subjects).publishToNats;
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
		return natsURL;
	}

	/**
	 * @return the logger
	 */
	protected Logger getLogger() {
		return logger;
	}
}
