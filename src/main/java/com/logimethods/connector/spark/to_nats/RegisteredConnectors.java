/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.util.LinkedList;
import java.util.List;

public class RegisteredConnectors {
	protected final int hashCode;
	protected final List<SparkToNatsConnector<?>> connectors = new LinkedList<SparkToNatsConnector<?>>();
	
	/**
	 * @param hashCode
	 * @param connectors
	 */
	public RegisteredConnectors(int hashCode) {
		super();
		this.hashCode = hashCode;
	}

	/**
	 * @return the hashCode
	 */
	protected int getHashCode() {
		return hashCode;
	}

	/**
	 * @return the connectors
	 */
	protected List<SparkToNatsConnector<?>> getConnectors() {
		return connectors;
	}
	
	protected void addConnector(SparkToNatsConnector<?> connector) {
		connectors.add(connector);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RegisteredConnectors [hashCode=" + hashCode + ", connectors=" + connectors + "]";
	}

}
