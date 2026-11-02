/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Soby Chacko
 * @since 2.3
 */
class RetryingDeserializerTests {

	@Test
	void basicRetryingDeserializer() {
		Deser delegate = new Deser();
		RetryingDeserializer<String> rdes = new RetryingDeserializer<>(delegate, new RetryTemplate());
		assertThat(rdes.deserialize("foo", "bar".getBytes())).isEqualTo("bar");
		assertThat(delegate.n).isEqualTo(3);
		delegate.n = 0;
		assertThat(rdes.deserialize("foo", new RecordHeaders(), "bar".getBytes())).isEqualTo("bar");
		assertThat(delegate.n).isEqualTo(3);
		delegate.n = 0;
		ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode("byteBuffer");
		assertThat(rdes.deserialize("foo", new RecordHeaders(), byteBuffer)).isEqualTo("byteBuffer");
		rdes.close();
	}

	@Test
	void retryingDeserializerWithRecoveryCallback() throws Exception {
		RetryingDeserializer<String> rdes = new RetryingDeserializer<>((s, b) -> {
			throw new RuntimeException();
		}, new RetryTemplate());
		Function<RetryException, String> recoveryCallback = mock();
		rdes.setRecoveryCallback(recoveryCallback);
		rdes.deserialize("my-topic", "my-data".getBytes());
		verify(recoveryCallback).apply(any(RetryException.class));
	}

	public static class Deser implements Deserializer<String> {

		int n;

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public String deserialize(String topic, byte[] data) {
			if (n++ < 2) {
				throw new RuntimeException();
			}
			return new String(data);
		}

		@Override
		public String deserialize(String topic, Headers headers, byte[] data) {
			if (n++ < 2) {
				throw new RuntimeException();
			}
			return new String(data);
		}

		@Override
		public String deserialize(String topic, Headers headers, ByteBuffer data) {
			if (n++ < 1) {
				throw new RuntimeException();
			}
			return new String(Utils.toArray(data));
		}

		@Override
		public void close() {
			// empty
		}

	}

}
