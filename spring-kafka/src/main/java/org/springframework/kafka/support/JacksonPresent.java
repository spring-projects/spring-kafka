/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support;

import org.springframework.util.ClassUtils;

/**
 * The utility to check if Jackson JSON processor is present in the classpath.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.3
 */
public final class JacksonPresent {

	private static final ClassLoader classLoader = ClassUtils.getDefaultClassLoader();  // NOSONAR

	private static final boolean jackson2Present = // NOSONAR
			ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader) &&
					ClassUtils.isPresent("com.fasterxml.jackson.core.JsonGenerator", classLoader);

	public static boolean isJackson2Present() {
		return jackson2Present;
	}

	private JacksonPresent() {
		super();
	}

}
