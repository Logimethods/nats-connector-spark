/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

import java.util.Collection;
import java.util.Properties;

import org.slf4j.Logger;

import com.logimethods.nats.connector.spark.Utilities;

public abstract class AbstractSparkToNatsConnector<T> {
	
	public static final String NATS_SUBJECTS = "nats.io.connector.spark2nats.subjects";

	public AbstractSparkToNatsConnector() {
		super();
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

	protected Properties getDefinedProperties() {
		if (getProperties() == null) {
			setProperties(new Properties(System.getProperties()));
		}
		return getProperties();
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