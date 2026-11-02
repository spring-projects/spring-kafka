/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * A package-protected header used to contain serialized
 * {@link DeserializationException}s. Only headers of this type will be examined for
 * deserialization.
 *
 * @author Gary Russell
 * @since 2.9.11
 */
class DeserializationExceptionHeader extends RecordHeader {

	/**
	 * Construct an instance with the provided properties.
	 * @param key the key.
	 * @param value the value;
	 */
	DeserializationExceptionHeader(String key, byte[] value) {
		super(key, value);
	}

}
