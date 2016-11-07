/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats_spark;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NatsSparkUtilities {
	/**
	 * @param elements
	 * @return
	 */
	public static List<String> transformIntoAList(String... elements) {
		ArrayList<String> list = new ArrayList<String>(elements.length);
		for (String element: elements){
			list.add(element.trim());
		}
		return list;
	}
	
	public static long generateUniqueID(Object obj) {
		return System.identityHashCode(obj) + Thread.currentThread().getId() + java.lang.System.currentTimeMillis();
	}
	
	public static long generateUniqueID() {
		return Thread.currentThread().getId() + java.lang.System.currentTimeMillis();
	}

	public static Collection<String> extractCollection(String str) {
		final String[] subjectsArray = str.split(",");
		return transformIntoAList(subjectsArray);
	}
	
	// @see https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
	// TODO (?) http://stackoverflow.com/questions/14619653/converting-a-float-to-a-byte-array-and-vice-versa-in-java : .order(ByteOrder.BIG_ENDIAN)
	public static byte[] encodeData(Object obj) {
		if (obj instanceof String) {
			return ((String) obj).getBytes();
		}
		if (obj instanceof Double) {
			return ByteBuffer.allocate(Double.BYTES).putDouble((Double) obj).array();
		}
		if (obj instanceof Float) {
			return ByteBuffer.allocate(Float.BYTES).putFloat((Float) obj).array();
		}
		if (obj instanceof Integer) {
			return ByteBuffer.allocate(Integer.BYTES).putInt((Integer) obj).array();
		}
		if (obj instanceof Long) {
			return ByteBuffer.allocate(Long.BYTES).putLong((Long) obj).array();
		}
		if (obj instanceof Byte) {
			return ByteBuffer.allocate(Byte.BYTES).put((Byte) obj).array();
		}
		if (obj instanceof Character) {
			return ByteBuffer.allocate(Character.BYTES).putChar((Character) obj).array();
		}
		if (obj instanceof Short) {
			return ByteBuffer.allocate(Short.BYTES).putShort((Short) obj).array();
		}
		throw new UnsupportedOperationException("It is not possible to encode Data of type " + obj.getClass());
	}
	
	// @see https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
	@SuppressWarnings("unchecked")
	public static <X> X extractData(Class<X> type, byte[] bytes) throws UnsupportedOperationException {
		if (type == String.class) {
			return (X) new String(bytes);
		}
		if ((type == Double.class) || (type == double.class)){
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			return (X) new Double(buffer.getDouble());
		}
		if ((type == Float.class) || (type == float.class)){
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			return (X) new Float(buffer.getFloat());
		}
		if ((type == Integer.class) || (type == int.class)){
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			return (X) new Integer(buffer.getInt());
		}
		if ((type == Long.class) || (type == long.class)){
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			return (X) new Long(buffer.getLong());
		}
		if ((type == Byte.class) || (type == byte.class)){
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			return (X) new Byte(buffer.get());
		}
		if ((type == Character.class) || (type == char.class)){
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			return (X) new Character(buffer.getChar());
		}
		if ((type == Short.class) || (type == short.class)){
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			return (X) new Short(buffer.getShort());
		}
		throw new UnsupportedOperationException("It is not possible to extract Data of type " + type);
	}
}
