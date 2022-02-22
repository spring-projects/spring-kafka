/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.kafka.support;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * A {@link ProducerInterceptor} that delegates to a collection of interceptors.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Soby Chacko
 *
 * @since 3.0
 *
 */
public class CompositeProducerInterceptor<K, V> implements ProducerInterceptor<K, V>, Closeable {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass())); //NOSONAR

	private final List<ProducerInterceptor<K, V>> delegates = new ArrayList<>();

	/**
	 * Construct an instance with the provided delegates to {@link ProducerInterceptor}s.
	 * @param delegates the delegates.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public CompositeProducerInterceptor(ProducerInterceptor<K, V>... delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates, "'delegates' cannot have null entries");
		this.delegates.addAll(Arrays.asList(delegates));
	}

	@Override
	public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
		ProducerRecord<K, V> interceptRecord = record;
		for (ProducerInterceptor<K, V> interceptor : this.delegates) {
			try {
				interceptRecord = interceptor.onSend(interceptRecord);
			}
			catch (Exception e) {
				// if exception thrown, log and continue calling other interceptors.
				if (record != null) {
					CompositeProducerInterceptor.this.logger.warn(e, () ->
							String.format("Error executing interceptor onSend callback for topic: %s, partition: %d",
									record.topic(), record.partition()));
				}
				else {
					CompositeProducerInterceptor.this.logger.warn(e, () -> "Error executing interceptor onSend callback: "
							+ interceptor.toString());
				}
			}
		}
		return interceptRecord;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		for (ProducerInterceptor<K, V> interceptor : this.delegates) {
			try {
				interceptor.onAcknowledgement(metadata, exception);
			}
			catch (Exception e) {
				// do not propagate interceptor exceptions, just log
				CompositeProducerInterceptor.this.logger.warn(e, () ->  "Error executing interceptor onAcknowledgement callback: "
						+ interceptor.toString());
			}
		}
	}

	@Override
	public void close() {
		for (ProducerInterceptor<K, V> interceptor : this.delegates) {
			try {
				interceptor.close();
			}
			catch (Exception e) {
				CompositeProducerInterceptor.this.logger.warn(e, () -> "Failed to close producer interceptor: "
						+ interceptor.toString());
			}
		}
	}

	@Override
	public void configure(Map<String, ?> configs) {
		this.delegates.forEach(delegate -> delegate.configure(configs));
	}
}
