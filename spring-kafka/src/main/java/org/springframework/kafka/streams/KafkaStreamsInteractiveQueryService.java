/*
 * Copyright 2024-2024 the original author or authors.
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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Provide a wrapper API around the interactive query stores in Kafka Streams.
 * Using this API, an application can gain access to a named state store in the
 * {@link KafkaStreams} under consideration.
 *
 * @author Soby Chacko
 * @since 3.2.0
 */
public class KafkaStreamsInteractiveQueryService {

	private static final int DEFAULT_MAX_ATTEMPTS = 1;
	/**
	 * {@link StreamsBuilderFactoryBean} that provides {@link KafkaStreams} where the state store is retrieved from.
	 */
	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	/**
	 * {@link RetryTemplate} to be used by the interative query service.
	 */
	private RetryTemplate retryTemplate = new RetryTemplate();

	/**
	 * Underlying {@link KafkaStreams} from {@link StreamsBuilderFactoryBean}.
	 */
	private KafkaStreams kafkaStreams;

	/**
	 * Constructs an instance for querying state stores from the KafkaStreams in the {@link StreamsBuilderFactoryBean}.
	 *
	 * @param streamsBuilderFactoryBean {@link StreamsBuilderFactoryBean} for {@link KafkaStreams}.
	 */
	public KafkaStreamsInteractiveQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
	}

	/**
	 * Retrieve and return a queryable store by name created in the application.
	 *
	 * @param storeName name of the queryable store
	 * @param storeType type of the queryable store
	 * @param <T> generic type for the queryable store
	 * @return queryable store.
	 */
	public <T> T retrieveQueryableStore(String storeName, QueryableStoreType<T> storeType) {
		if (this.kafkaStreams == null) {
			this.kafkaStreams = this.streamsBuilderFactoryBean.getKafkaStreams();
		}
		Assert.notNull(this.kafkaStreams, "KafkaStreams cannot be null");
		StoreQueryParameters<T> storeQueryParams = StoreQueryParameters.fromNameAndType(storeName, storeType);
		AtomicReference<StoreQueryParameters<T>> storeQueryParametersReference = new AtomicReference<>(storeQueryParams);

		return getRetryTemplate().execute(context -> {
			try {
				return this.kafkaStreams.store(storeQueryParametersReference.get());
			}
			catch (Exception e) {
				throw new IllegalStateException("Error retrieving state store: " + storeName, e);
			}
		});
	}

	private RetryTemplate getRetryTemplate() {
		if (this.retryTemplate == null) {
			this.retryTemplate = new RetryTemplate();
			retryTemplate.setBackOffPolicy(new FixedBackOffPolicy());
			RetryPolicy retryPolicy = new SimpleRetryPolicy(DEFAULT_MAX_ATTEMPTS);
			retryTemplate.setRetryPolicy(retryPolicy);
		}
		return this.retryTemplate;
	}

	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}
}
