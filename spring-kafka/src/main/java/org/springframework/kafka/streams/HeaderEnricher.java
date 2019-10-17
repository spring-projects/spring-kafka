/*
 * Copyright 2019 the original author or authors.
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.expression.Expression;

/**
 * Manipulate the headers.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class HeaderEnricher<K, V> implements Transformer<K, V, KeyValue<K, V>> {

	private final Map<String, Expression> headerExpressions = new HashMap<>();

	private ProcessorContext processorContext;

	public HeaderEnricher(Map<String, Expression> headerExpressions) {
		this.headerExpressions.putAll(headerExpressions);
	}

	@Override
	public void init(ProcessorContext context) {
		this.processorContext = context;
	}

	@Override
	public KeyValue<K, V> transform(K key, V value) {
		Headers headers = this.processorContext.headers();
		Container<K, V> container = new Container<>(this.processorContext, key, value);
		this.headerExpressions.forEach((name, expression) -> {
			Object headerValue = expression.getValue(container);
			if (headerValue instanceof String) {
				headerValue = ((String) headerValue).getBytes(StandardCharsets.UTF_8);
			}
			else if (!(headerValue instanceof byte[])) {
				throw new IllegalStateException("Invalid header value type" + headerValue.getClass());
			}
			headers.add(new RecordHeader(name, (byte[]) headerValue));
		});
		return new KeyValue<>(key, value);
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

		private final ProcessorContext context;

		private final K key;

		private final V value;

		private Container(ProcessorContext context, K key, V value) {
			this.context = context;
			this.key = key;
			this.value = value;
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

	}

}
