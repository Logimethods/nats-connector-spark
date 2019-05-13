/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkToNatsValidator implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static java.util.function.Function<String,  byte[]> getBytes = (java.util.function.Function<String,  byte[]> & Serializable) str -> str.getBytes();


	/**
	 * @return
	 */
	public static JavaDStream<Integer> generateIntegers(JavaDStream<String> lines) {
		return lines.map(str -> Integer.parseInt(str));
	}

	/**
	 * @param subject1
	 * @param rdd
	 * @return
	 */
	public static JavaRDD<Tuple2<String, Integer>> newSubjectStringTuple(String subject1, JavaRDD<Integer> rdd) {
		return rdd.map((Function<Integer, Tuple2<String, Integer>>) str -> {
							return new Tuple2<String, Integer>(subject1, str);
						});
	}

	/**
	 * @param subject1
	 * @param rdd
	 * @return
	 */
	public static JavaRDD<Tuple2<String, Integer>> newSubjectDotStringTuple(String subject1, JavaRDD<Integer> rdd) {
		return rdd.map((Function<Integer, Tuple2<String, Integer>>) str -> {
							return new Tuple2<String, Integer>(subject1 + "." + str, str);
						});
	}

	public static JavaPairDStream<String, String> getJavaPairDStream(final JavaDStream<String> lines, final JavaStreamingContext ssc, final String subject1) {
	//-	final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());
		return lines.mapToPair((PairFunction<String, String, String>) str -> {
							return new Tuple2<String, String>(subject1 + "." + str, str);
						});
	}
}
