/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;

/**
 * Determine the {@link JavaType} from the topic/data/headers.
 *
 * @author Gary Russell
 * @author Soby Chacko
 *
 * @since 4.0
 */
@FunctionalInterface
public interface JacksonJsonTypeResolver {

	/**
	 * Determine the type.
	 * @param topic the topic.
	 * @param data the serialized data.
	 * @param headers the headers.
	 * @return the type.
	 */
	JavaType resolveType(String topic, byte[] data, @Nullable Headers headers);

}
