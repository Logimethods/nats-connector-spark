/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats_spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Utilities {
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
	
	public static long generateUniqueID() {
		return Thread.currentThread().getId() + java.lang.System.currentTimeMillis();
	}

	public static Collection<String> extractCollection(String str) {
		final String[] subjectsArray = str.split(",");
		return transformIntoAList(subjectsArray);
	}
}
