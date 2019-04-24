/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.JavaDStream;

public class SparkToNatsStreamingValidator implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @return
	 */
	public static JavaDStream<Integer> generateIntegers(JavaDStream<String> lines) {
		return lines.map(str -> Integer.parseInt(str));
	}
}
