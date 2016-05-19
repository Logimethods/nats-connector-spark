/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import java.util.ArrayList;
import java.util.List;

public class Utilities {
	/**
	 * @param subjects
	 * @return
	 */
	public static List<String> transformIntoAList(String... subjects) {
		ArrayList<String> list = new ArrayList<String>(subjects.length);
		for (String subject: subjects){
			list.add(subject.trim());
		}
		return list;
	}
}
