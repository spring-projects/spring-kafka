/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.streams;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import org.springframework.expression.Expression;

/**
 * Manipulate the headers.
 *
 * @param <K> the input key type.
 * @param <V> the input value type.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class HeaderEnricherProcessor<K, V> extends ContextualProcessor<K, V, K, V> {

	private final Map<String, Expression> headerExpressions = new HashMap<>();

	/**
	 * Construct an instance with the provided header expressions.
	 * @param headerExpressions the header expressions; name:expression.
	 */
	public HeaderEnricherProcessor(Map<String, Expression> headerExpressions) {
		this.headerExpressions.putAll(headerExpressions);
	}

	@Override
	public void process(Record<K, V> record) {
		Headers headers = record.headers();
		Container<K, V> container = new Container<>(context(), record.key(), record.value(), record);
		this.headerExpressions.forEach((name, expression) -> {
			Object headerValue = expression.getValue(container);
			if (headerValue instanceof String) {
				headerValue = ((String) headerValue).getBytes(StandardCharsets.UTF_8);
			}
			else if (!(headerValue instanceof byte[])) {
				if (headerValue != null) {
					throw new IllegalStateException("Invalid header value type: " + headerValue.getClass());
				}
				else {
					throw new IllegalStateException("headerValue is null");
				}
			}
			headers.add(new RecordHeader(name, (byte[]) headerValue));
		});
		context().forward(record);
	}

	@Override
	public void close() {
		// NO-OP
	}

	/**
	 * Container object for SpEL evaluation.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 */
	public static final class Container<K, V> {

		private final ProcessorContext<K, V> context;

		private final K key;

		private final V value;

		private final Record<K, V> record;

		Container(ProcessorContext<K, V> context, K key, V value, Record<K, V> record) {
			this.context = context;
			this.key = key;
			this.value = value;
			this.record = record;
		}

		public ProcessorContext<K, V> getContext() {
			return this.context;
		}

		public K getKey() {
			return this.key;
		}

		public V getValue() {
			return this.value;
		}

		public Record<K, V> getRecord() {
			return this.record;
		}

	}

}
