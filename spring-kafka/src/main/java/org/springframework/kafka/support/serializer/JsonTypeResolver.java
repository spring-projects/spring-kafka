/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import com.fasterxml.jackson.databind.JavaType;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

/**
 * Determine the {@link JavaType} from the topic/data/headers.
 *
 * @author Gary Russell
 * @since 2.5.3
 *
 * @deprecated since 4.0 in favor of {@link JacksonJsonTypeResolver} for Jackson 3.
 */
@Deprecated(forRemoval = true, since = "4.0")
@FunctionalInterface
public interface JsonTypeResolver {

	/**
	 * Determine the type.
	 * @param topic the topic.
	 * @param data the serialized data.
	 * @param headers the headers.
	 * @return the type.
	 */
	JavaType resolveType(String topic, byte[] data, @Nullable Headers headers);

}
