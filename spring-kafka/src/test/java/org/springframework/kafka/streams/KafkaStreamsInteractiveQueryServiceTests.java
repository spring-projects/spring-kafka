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

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 * @author Sanghyeok An
 * @since 3.2.0
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1,
		topics = { "iqs-test-in", "iqs-test-out" })
class KafkaStreamsInteractiveQueryServiceTests {

	public static final String IQS_TEST_IN = "iqs-test-in";

	public static final String IQS_TEST_OUT = "iqs-test-out";

	public static final String STATE_STORE = "my-state-store";

	public static final String NON_EXISTENT_STORE = "my-non-existent-store";

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	private KafkaStreamsInteractiveQueryService interactiveQueryService;

	@Autowired
	private CompletableFuture<ConsumerRecord<?, String>> resultFuture;

	@Test
	void retrieveQueryableStore() throws Exception {
		ensureKafkaStreamsProcessorIsUpAndRunning();

		ReadOnlyKeyValueStore<Object, Object> objectObjectReadOnlyKeyValueStore = this.interactiveQueryService
				.retrieveQueryableStore(STATE_STORE,
						QueryableStoreTypes.keyValueStore());

		assertThat((Long) objectObjectReadOnlyKeyValueStore.get(123)).isGreaterThanOrEqualTo(1);
	}

	private void ensureKafkaStreamsProcessorIsUpAndRunning() throws InterruptedException, ExecutionException, TimeoutException {
		this.kafkaTemplate.sendDefault(123, "123");
		this.kafkaTemplate.flush();

		ConsumerRecord<?, String> result = resultFuture.get(600, TimeUnit.SECONDS);
		assertThat(result).isNotNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	void retrieveNonExistentStateStoreAndVerifyRetries() throws Exception {
		ensureKafkaStreamsProcessorIsUpAndRunning();

		assertThat(this.streamsBuilderFactoryBean.getKafkaStreams()).isNotNull();
		KafkaStreams kafkaStreams = spy(this.streamsBuilderFactoryBean.getKafkaStreams());
		assertThat(kafkaStreams).isNotNull();

		Field kafkaStreamsField = KafkaStreamsInteractiveQueryService.class.getDeclaredField("kafkaStreams");
		kafkaStreamsField.setAccessible(true);
		kafkaStreamsField.set(interactiveQueryService, kafkaStreams);

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> {
					this.interactiveQueryService
					.retrieveQueryableStore(NON_EXISTENT_STORE, QueryableStoreTypes.keyValueStore());
				})
				.withMessageContaining("Error retrieving state store: my-non-existent-store");

		verify(kafkaStreams, times(3)).store(any(StoreQueryParameters.class));
	}

	@Test
	void currentHostInfo() {
		HostInfo currentKafkaStreamsApplicationHostInfo =
				this.interactiveQueryService.getCurrentKafkaStreamsApplicationHostInfo();
		assertThat(currentKafkaStreamsApplicationHostInfo.host()).isEqualTo("localhost");
		assertThat(currentKafkaStreamsApplicationHostInfo.port()).isEqualTo(8080);
	}

	@Test
	void hostInfoForKeyAndStore() throws Exception {
		ensureKafkaStreamsProcessorIsUpAndRunning();

		HostInfo kafkaStreamsApplicationHostInfo =
				this.interactiveQueryService.getKafkaStreamsApplicationHostInfo(STATE_STORE, 123,
						new IntegerSerializer());
		// In real applications, the above call may return a different server than what is configured
		// via application.server on the Kafka Streams where the call was invoked. However, in the case
		// of this test, we only have a single Kafka Streams instance and even there, we provide a mock
		// value for application.server (localhost:8080). Because of that, that is what we are verifying against.
		assertThat(kafkaStreamsApplicationHostInfo.host()).isEqualTo("localhost");
		assertThat(kafkaStreamsApplicationHostInfo.port()).isEqualTo(8080);
	}

	@Test
	void hostInfoForNonExistentKeyAndStateStore() throws Exception {
		ensureKafkaStreamsProcessorIsUpAndRunning();

		assertThat(this.streamsBuilderFactoryBean.getKafkaStreams()).isNotNull();
		KafkaStreams kafkaStreams = spy(this.streamsBuilderFactoryBean.getKafkaStreams());
		assertThat(kafkaStreams).isNotNull();

		Field kafkaStreamsField = KafkaStreamsInteractiveQueryService.class.getDeclaredField("kafkaStreams");
		kafkaStreamsField.setAccessible(true);
		kafkaStreamsField.set(interactiveQueryService, kafkaStreams);

		IntegerSerializer serializer = new IntegerSerializer();

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> {
					this.interactiveQueryService.getKafkaStreamsApplicationHostInfo(NON_EXISTENT_STORE, 12345,
							serializer);
				})
				.withMessage("Error when retrieving state store.")
				.havingCause().withMessage("KeyQueryMetadata is not yet available.");

		verify(kafkaStreams, times(3)).queryMetadataForKey(NON_EXISTENT_STORE, 12345,
				serializer);
	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfig {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(this.brokerAddresses);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory(), true);
			kafkaTemplate.setDefaultTopic("iqs-test-in");
			return kafkaTemplate;
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			return KafkaTestUtils.consumerProps(this.brokerAddresses, "testGroup", false);
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
		kafkaListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = KafkaTestUtils.streamsProps(
					"iqs-testStreams", this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
					WallclockTimestampExtractor.class.getName());
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
			props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080");
			return new KafkaStreamsConfiguration(props);
		}

		@Bean
		public KStream<Integer, String> kStream(StreamsBuilder kStreamBuilder) {
			KStream<Integer, String> input = kStreamBuilder.stream(IQS_TEST_IN);
			final KStream<Integer, String> outbound = input.filter((key, value) -> key == 123)
					.map((key, value) -> new KeyValue<>(123, value))
					.groupByKey(Grouped.with(new Serdes.IntegerSerde(),
							new Serdes.StringSerde()))
					.count(Materialized.as(STATE_STORE)).toStream()
					.map((key, value) -> new KeyValue<>(key, "Count for ID 123: " + value));
			outbound.to(IQS_TEST_OUT);
			return outbound;
		}

		@Bean
		public KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
			final KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService =
					new KafkaStreamsInteractiveQueryService(streamsBuilderFactoryBean);
			RetryTemplate retryTemplate = new RetryTemplate();
			retryTemplate.setRetryPolicy(RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build());
			kafkaStreamsInteractiveQueryService.setRetryTemplate(retryTemplate);
			return kafkaStreamsInteractiveQueryService;
		}

		@Bean
		public CompletableFuture<ConsumerRecord<?, String>> resultFuture() {
			return new CompletableFuture<>();
		}

		@KafkaListener(topics = "iqs-test-out")
		public void listener(ConsumerRecord<?, String> payload) {
			resultFuture().complete(payload);
		}

	}

}
