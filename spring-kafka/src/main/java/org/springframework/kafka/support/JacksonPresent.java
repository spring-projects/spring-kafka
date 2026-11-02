/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.support;

import org.jspecify.annotations.Nullable;

import org.springframework.util.ClassUtils;

/**
 * The utility to check if Jackson JSON processor is present in the classpath.
 *
 * @author Artem Bilan
 * @author Gary Russell
 * @author Soby Chacko
 *
 * @since 1.3
 */
public final class JacksonPresent {

	private static final @Nullable ClassLoader classLoader = ClassUtils.getDefaultClassLoader();  // NOSONAR

	private static final boolean jackson2Present = // NOSONAR
			ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader) &&
					ClassUtils.isPresent("com.fasterxml.jackson.core.JsonGenerator", classLoader);

	private static final boolean jackson3Present = // NOSONAR
			ClassUtils.isPresent("tools.jackson.databind.ObjectMapper", classLoader) &&
					ClassUtils.isPresent("tools.jackson.core.JsonGenerator", classLoader);

	public static boolean isJackson2Present() {
		return jackson2Present;
	}

	public static boolean isJackson3Present() {
		return jackson3Present;
	}

	private JacksonPresent() {
	}

}
