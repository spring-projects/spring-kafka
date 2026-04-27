/*
 * Copyright 2019-present the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Soby Chacko
 * @author Sanghyeok An
 * @since 2.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "recoverer1", "recoverer2", "recovererDLQ" })
public class RecoveringDeserializationExceptionHandlerTests
		extends AbstractRecoveringExceptionHandlerTests<RecoveringDeserializationExceptionHandler,
		DeserializationExceptionHandler.Response> {

	@Autowired
	private KafkaOperations<byte[], byte[]> kafkaTemplate;

	@Autowired
	private CompletableFuture<ConsumerRecord<byte[], byte[]>> resultFuture;

	private final ConsumerRecord<byte[], byte[]> record;

	RecoveringDeserializationExceptionHandlerTests() {
		super(RecoveringDeserializationExceptionHandler.RECOVERER,
				RecoveringDeserializationExceptionHandler.DLQ_DESTINATION_RESOLVER);
		RecordHeaders headers = new RecordHeaders();
		headers.add("original-header", "original-header-value".getBytes());
		this.record = new ConsumerRecord<>(
				"source-topic",
				2,
				42L,
				150L,
				TimestampType.CREATE_TIME,
				0,
				0,
				new byte[0],
				new byte[0],
				headers,
				Optional.empty(),
				Optional.empty());
	}

	@Override
	protected RecoveringDeserializationExceptionHandler createHandler(Map<String, Object> configs) {
		RecoveringDeserializationExceptionHandler handler = new RecoveringDeserializationExceptionHandler();
		handler.configure(configs);
		return handler;
	}

	@Override
	protected DeserializationExceptionHandler.Response handleError(
			RecoveringDeserializationExceptionHandler handler, ErrorHandlerContext context, Exception exception) {
		return handler.handleError(context, this.record, exception);
	}

	@Override
	protected ConsumerRecord<?, ?> createDestinationResolverConsumerRecord(ErrorHandlerContext context) {
		return this.record;
	}

	@Override
	protected void assertResponseShouldResume(DeserializationExceptionHandler.Response response) {
		assertThat(response.result()).isEqualTo(DeserializationExceptionHandler.Result.RESUME);
	}

	@Override
	protected void assertResponseShouldFail(DeserializationExceptionHandler.Response response) {
		assertThat(response.result()).isEqualTo(DeserializationExceptionHandler.Result.FAIL);
	}

	@Override
	protected void assertResponseShouldContainDeadLetterRecords(
			DeserializationExceptionHandler.Response response, ProducerRecord<byte[], byte[]> expectedRecord) {
		assertThat(response.deadLetterQueueRecords()).hasSize(1).first()
				.satisfies(deadLetterRecord -> {
					assertThat(deadLetterRecord.topic()).isEqualTo(expectedRecord.topic());
					assertThat(deadLetterRecord.partition()).isEqualTo(expectedRecord.partition());
					assertThat(deadLetterRecord.key()).isEqualTo(expectedRecord.key());
					assertThat(deadLetterRecord.value()).isEqualTo(expectedRecord.value());
					assertThat(deadLetterRecord.headers().toArray().length).isEqualTo(9);
					expectedRecord.headers().forEach(expectedHeader ->
							assertThat(deadLetterRecord.headers().lastHeader(expectedHeader.key()))
									.isNotNull()
									.satisfies(h -> assertThat(h.value()).isEqualTo(expectedHeader.value())));
					// Do not validate content for the following headers, only presence
					assertThat(deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
					assertThat(deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
					assertThat(deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();
				});
	}

	@Test
	@SuppressWarnings("removal")
	void withLegacyRecovererKeyAsStringProperty() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, Recoverer.class.getName());
		RecoveringDeserializationExceptionHandler handler = createHandler(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isInstanceOf(Recoverer.class);
		assertResponseShouldResume(handleError(handler, createMockContext(), new IllegalArgumentException()));
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalStateException()));
	}

	@Test
	@SuppressWarnings("removal")
	void withLegacyRecovererKeyAsClassProperty() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, Recoverer.class);
		RecoveringDeserializationExceptionHandler handler = createHandler(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isInstanceOf(Recoverer.class);
		assertResponseShouldResume(handleError(handler, createMockContext(), new IllegalArgumentException()));
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalStateException()));
	}

	@Test
	@SuppressWarnings("removal")
	void withLegacyRecovererKeyAsObjectProperty() {
		Map<String, Object> configs = new HashMap<>();
		Recoverer rec = new Recoverer();
		configs.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, rec);
		RecoveringDeserializationExceptionHandler handler = createHandler(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isSameAs(rec);
		assertResponseShouldResume(handleError(handler, createMockContext(), new IllegalArgumentException()));
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalStateException()));
	}

	@Test
	void integration() throws Exception {
		this.kafkaTemplate.send("recoverer1", "foo".getBytes(), "bar".getBytes());
		ConsumerRecord<byte[], byte[]> record = this.resultFuture.get(10, TimeUnit.SECONDS);
		assertThat(record).isNotNull();
		assertThat(record.key()).isEqualTo("foo".getBytes());
		assertThat(record.value()).isEqualTo("bar".getBytes());
		assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();
	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfig {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean
		public ProducerFactory<byte[], byte[]> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(this.brokerAddresses);
			senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			return senderProps;
		}

		@Bean
		public KafkaTemplate<byte[], byte[]> template() {
			KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFactory(), true);
			kafkaTemplate.setDefaultTopic("recoverer1");
			return kafkaTemplate;
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = KafkaTestUtils.streamsProps("testStreams", this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, FailSerde.class);
			props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
					WallclockTimestampExtractor.class.getName());
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
			props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					RecoveringDeserializationExceptionHandler.class);
			props.put(RecoveringDeserializationExceptionHandler.RECOVERER, recoverer());
			return new KafkaStreamsConfiguration(props);
		}

		@Bean
		public DeadLetterPublishingRecoverer recoverer() {
			return new DeadLetterPublishingRecoverer(template(),
					(record, ex) -> new TopicPartition("recovererDLQ", -1));
		}

		@Bean
		public KStream<byte[], byte[]> kStream(StreamsBuilder kStreamBuilder) {
			KStream<byte[], byte[]> stream = kStreamBuilder.stream("recoverer1");
			stream.to("recoverer2");
			return stream;
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.brokerAddresses, "recovererGroup",
					false);
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			return consumerProps;
		}

		@Bean
		public ConsumerFactory<byte[], byte[]> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<byte[], byte[]>>
				kafkaListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public CompletableFuture<ConsumerRecord<byte[], byte[]>> resultFuture() {
			return new CompletableFuture<>();
		}

		@KafkaListener(topics = "recovererDLQ")
		public void listener(ConsumerRecord<byte[], byte[]> payload) {
			resultFuture().complete(payload);
		}

	}

	public static class FailSerde implements Serde<byte[]> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {

		}

		@Override
		public void close() {
		}

		@Override
		public Serializer<byte[]> serializer() {
			return null;
		}

		@Override
		public Deserializer<byte[]> deserializer() {
			return new Deserializer<byte[]>() {

				private boolean key;

				@Override
				public void configure(Map<String, ?> configs, boolean isKey) {
					this.key = isKey;
				}

				@Override
				public byte[] deserialize(String topic, byte[] data) {
					if (this.key) {
						return data;
					}
					else {
						throw new IllegalStateException("Intentional");
					}
				}

				@Override
				public void close() {
				}

			};
		}

	}

}
