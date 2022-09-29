/*
 * Copyright 2019-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
				throw new IllegalStateException("Invalid header value type: " + headerValue.getClass());
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

		private final Record record;

		Container(ProcessorContext<K, V> context, K key, V value, Record record) {
			this.context = context;
			this.key = key;
			this.value = value;
			this.record = record;
		}

		public ProcessorContext getContext() {
			return this.context;
		}

		public K getKey() {
			return this.key;
		}

		public V getValue() {
			return this.value;
		}

		public Record getRecord() {
			return this.record;
		}

	}

}
