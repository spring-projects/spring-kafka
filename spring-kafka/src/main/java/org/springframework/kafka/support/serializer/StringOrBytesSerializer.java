/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

/**
 * A serializer that can handle {@code byte[]}, {@link Bytes} and {@link String}.
 * Convenient when used with one of the Json message converters.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 2.3
 *
 */
public class StringOrBytesSerializer implements Serializer<Object> {

	private final StringSerializer stringSerializer = new StringSerializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.stringSerializer.configure(configs, isKey);
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	@Override
	public byte[] serialize(String topic, Object data) {
		if (data == null) {
			return null;
		}

		if (data instanceof byte[] bytes) {
			return bytes;
		}

		if (data instanceof Bytes bytes) {
			return bytes.get();
		}

		if (data instanceof String string) {
			return this.stringSerializer.serialize(topic, string);
		}

		throw new IllegalStateException("This serializer can only handle byte[], Bytes or String values");
	}

	@Override
	public void close() {
		this.stringSerializer.close();
	}

}
