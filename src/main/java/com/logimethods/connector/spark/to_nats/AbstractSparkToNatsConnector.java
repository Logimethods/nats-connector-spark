/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;

import org.slf4j.Logger;

import com.logimethods.connector.nats_spark.Utilities;

public abstract class AbstractSparkToNatsConnector<T> implements Serializable {
	
	public static final String NATS_SUBJECTS = "nats.io.connector.spark2nats.subjects";
//	public static final String NATS_URL = "nats.io.connector.spark2nats.server_url";

	/**
	 * 
	 */
	protected AbstractSparkToNatsConnector() {
		super();
	}

	public AbstractSparkToNatsConnector(String natsURL, Properties properties, String... subjects) {
		super();
		setProperties(properties);
		setSubjects(Utilities.transformIntoAList(subjects));
		setNatsURL(natsURL);		
	}

	public AbstractSparkToNatsConnector(String natsURL, Properties properties, Collection<String> subjects) {
		super();
		setProperties(properties);
		setSubjects(subjects);
		setNatsURL(natsURL);		
	}
	
	protected abstract Logger getLogger();

	/**
	 * @param connection the connection to set
	 */
//	protected abstract void setConnection(Connection connection);
//	protected abstract Connection getConnection();
	
	/**
	 * @param properties the properties to set
	 */
	protected abstract void setProperties(Properties properties);	
	protected abstract Properties getProperties();

	/**
	 * @param subjects the subjects to set
	 */
	protected abstract void setSubjects(Collection<String> subjects);	
	protected abstract Collection<String> getSubjects();

	/**
	 * @param subjects the subjects to set
	 */
	protected abstract void setNatsURL(String natsURL);	
	protected abstract String getNatsURL();

	/**
	 * @param properties the properties to set
	 */
	@SuppressWarnings("unchecked")
	public T withProperties(Properties properties) {
		setProperties(properties);
		return (T)this;
	}

	/**
	 * @param subjects the subjects to set
	 */
	@SuppressWarnings("unchecked")
	public T withSubjects(String... subjects) {
		setSubjects(Utilities.transformIntoAList(subjects));
		return (T)this;
	}

	/**
	 * @param natsURL the NATS URL to set
	 */
	@SuppressWarnings("unchecked")
	public T withNatsURL(String natsURL) {
		setNatsURL(natsURL);
		return (T)this;
	}

	protected Collection<String> getDefinedSubjects() throws Exception {
		if ((getSubjects() ==  null) || (getSubjects().size() == 0)) {
			final String subjectsStr = getProperties().getProperty(NATS_SUBJECTS);
			if (subjectsStr == null) {
				throw new Exception("SparkToNatsConnector needs at least one NATS Subject.");
			}
			final String[] subjectsArray = subjectsStr.split(",");
			setSubjects(Utilities.transformIntoAList(subjectsArray));
			getLogger().debug("Subject provided by the Properties: '{}'", getSubjects());
		}
		return getSubjects();
	}
}