/*
 * Copyright 2024-present the original author or authors.
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

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.jspecify.annotations.Nullable;

import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Provide a wrapper API around the interactive query stores in Kafka Streams.
 * Using this API, an application can gain access to a named state store in the
 * {@link KafkaStreams} under consideration.
 *
 * @author Soby Chacko
 * @author Stephane Nicoll
 *
 * @since 3.2
 */
public class KafkaStreamsInteractiveQueryService {

	/**
	 * {@link StreamsBuilderFactoryBean} that provides {@link KafkaStreams} where the state store is retrieved from.
	 */
	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	/**
	 * {@link RetryTemplate} to be used by the interactive query service.
	 */
	private RetryTemplate retryTemplate = new RetryTemplate();

	/**
	 * Underlying {@link KafkaStreams} from {@link StreamsBuilderFactoryBean}.
	 */
	@SuppressWarnings("NullAway.Init")
	private volatile KafkaStreams kafkaStreams;

	/**
	 * Construct an instance for querying state stores from the KafkaStreams in the {@link StreamsBuilderFactoryBean}.
	 * @param streamsBuilderFactoryBean {@link StreamsBuilderFactoryBean} for {@link KafkaStreams}.
	 */
	public KafkaStreamsInteractiveQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		Assert.notNull(streamsBuilderFactoryBean, "StreamsBuildFactoryBean instance cannot be null.");
		this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
	}

	/**
	 * Custom {@link RetryTemplate} provided by the end users.
	 * @param retryTemplate {@link RetryTemplate}
	 */
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		Assert.notNull(retryTemplate, "The provided RetryTemplate instance must not be null");
		this.retryTemplate = retryTemplate;
	}

	/**
	 * Retrieve and return a queryable store by name created in the application.
	 * @param storeName name of the queryable store
	 * @param storeType type of the queryable store
	 * @param <T> generic type for the queryable store
	 * @return queryable store.
	 */
	public <T> T retrieveQueryableStore(String storeName, QueryableStoreType<T> storeType) {
		populateKafkaStreams();
		StoreQueryParameters<T> storeQueryParams = StoreQueryParameters.fromNameAndType(storeName, storeType);

		try {
			return Objects.requireNonNull(this.retryTemplate.execute(() -> this.kafkaStreams.store(storeQueryParams)));
		}
		catch (RetryException ex) {
			throw new IllegalStateException("Error retrieving state store: " + storeName, ex.getCause());
		}
	}

	private void populateKafkaStreams() {
		KafkaStreams kafkaStreamsToUse = this.kafkaStreams;
		if (kafkaStreamsToUse == null) {
			kafkaStreamsToUse = this.streamsBuilderFactoryBean.getKafkaStreams();
			Assert.notNull(kafkaStreamsToUse, "KafkaStreams cannot be null. " +
					"Make sure that the corresponding StreamsBuilderFactoryBean has started properly.");
			this.kafkaStreams = kafkaStreamsToUse;
		}
	}

	/**
	 * Retrieve the current {@link HostInfo} where this Kafka Streams application is running on.
	 * This {link @HostInfo} is different from the Kafka `bootstrap.server` property and is based on
	 * the Kafka Streams configuration property `application.server` where user-defined REST
	 * endpoints can be invoked per each Kafka Streams application instance.
	 * If this property - `application.server` - is not available from the end-user application, then null is returned.
	 * @return the current {@link HostInfo}
	 */
	@Nullable
	public HostInfo getCurrentKafkaStreamsApplicationHostInfo() {
		Properties streamsConfiguration = this.streamsBuilderFactoryBean
				.getStreamsConfiguration();
		if (streamsConfiguration != null && streamsConfiguration.containsKey("application.server")) {
			String applicationServer = (String) streamsConfiguration.get("application.server");
			String[] appServerComponents = StringUtils.split(applicationServer, ":");
			if (appServerComponents != null) {
				return new HostInfo(appServerComponents[0], Integer.parseInt(appServerComponents[1]));
			}
		}
		return null;
	}

	/**
	 * Retrieve the {@link HostInfo} where the provided store and key are hosted on. This may
	 * not be the current host that is running the application. Kafka Streams will look
	 * through all the consumer instances under the same application id and retrieve the
	 * proper host. Note that the end user applications must provide `application.server` as a
	 * configuration property for all the application instances when calling this method.
	 * If this is not available, then null maybe returned.
	 * @param <K> generic type for key
	 * @param store store name
	 * @param key key to look for
	 * @param serializer {@link Serializer} for the key
	 * @return the {@link HostInfo} where the key for the provided store is hosted currently
	 */
	public <K> HostInfo getKafkaStreamsApplicationHostInfo(String store, K key, Serializer<K> serializer) {
		populateKafkaStreams();
		try {
			return getActiveHost(store, key, serializer);
		}
		catch (RetryException ex) {
			throw new IllegalStateException("Error when retrieving state store.", ex.getCause());
		}
	}

	private <K> HostInfo getActiveHost(String store, K key, Serializer<K> serializer) throws RetryException {
		return Objects.requireNonNull(this.retryTemplate.execute(() -> {
			KeyQueryMetadata keyQueryMetadata = this.kafkaStreams.queryMetadataForKey(store, key, serializer);
			if (keyQueryMetadata != null) {
				return keyQueryMetadata.activeHost();
			}
			throw new IllegalStateException("KeyQueryMetadata is not yet available.");
		}));
	}

}
