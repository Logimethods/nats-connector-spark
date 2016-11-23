/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats_spark;

/**
 * An Exception that refers to missing data (to define a connection to NATS, like the Subjects).
 */
public class IncompleteException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public IncompleteException() {
	}

	public IncompleteException(String message) {
		super(message);
	}

	public IncompleteException(Throwable cause) {
		super(cause);
	}

	public IncompleteException(String message, Throwable cause) {
		super(message, cause);
	}

	public IncompleteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
