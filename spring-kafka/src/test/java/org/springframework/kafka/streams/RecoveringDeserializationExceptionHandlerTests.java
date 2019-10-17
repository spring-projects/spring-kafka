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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "recoverer1", "recoverer2", "recovererDLQ" })
public class RecoveringDeserializationExceptionHandlerTests {

	@Autowired
	private KafkaTemplate<byte[], byte[]> kafkaTemplate;

	@Autowired
	private SettableListenableFuture<ConsumerRecord<byte[], byte[]>> resultFuture;

	@Test
	void viaStringProperty() {
		RecoveringDeserializationExceptionHandler handler = new RecoveringDeserializationExceptionHandler();
		Map<String, Object> configs = new HashMap<String, Object>();
		configs.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER,
				Recoverer.class.getName());
		handler.configure(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isInstanceOf(Recoverer.class);
		assertThat(handler.handle(null, new ConsumerRecord<byte[], byte[]>("foo", 0, 0, null, null),
				new IllegalArgumentException())).isEqualTo(DeserializationHandlerResponse.CONTINUE);
		assertThat(handler.handle(null, new ConsumerRecord<byte[], byte[]>("foo", 0, 0, null, null),
				new IllegalStateException())).isEqualTo(DeserializationHandlerResponse.FAIL);
	}

	@Test
	void viaClassProperty() {
		RecoveringDeserializationExceptionHandler handler = new RecoveringDeserializationExceptionHandler();
		Map<String, Object> configs = new HashMap<String, Object>();
		configs.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, Recoverer.class);
		handler.configure(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isInstanceOf(Recoverer.class);
		assertThat(handler.handle(null, new ConsumerRecord<byte[], byte[]>("foo", 0, 0, null, null),
				new IllegalArgumentException())).isEqualTo(DeserializationHandlerResponse.CONTINUE);
		assertThat(handler.handle(null, new ConsumerRecord<byte[], byte[]>("foo", 0, 0, null, null),
				new IllegalStateException())).isEqualTo(DeserializationHandlerResponse.FAIL);
	}

	@Test
	void viaObjectProperty() {
		RecoveringDeserializationExceptionHandler handler = new RecoveringDeserializationExceptionHandler();
		Map<String, Object> configs = new HashMap<String, Object>();
		Recoverer rec = new Recoverer();
		configs.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, rec);
		handler.configure(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isSameAs(rec);
		assertThat(handler.handle(null, new ConsumerRecord<byte[], byte[]>("foo", 0, 0, null, null),
				new IllegalArgumentException())).isEqualTo(DeserializationHandlerResponse.CONTINUE);
		assertThat(handler.handle(null, new ConsumerRecord<byte[], byte[]>("foo", 0, 0, null, null),
				new IllegalStateException())).isEqualTo(DeserializationHandlerResponse.FAIL);
	}

	@Test
	void withNoRecoverer() {
		RecoveringDeserializationExceptionHandler handler = new RecoveringDeserializationExceptionHandler();
		assertThat(handler.handle(null, new ConsumerRecord<byte[], byte[]>("foo", 0, 0, null, null),
				new IllegalArgumentException())).isEqualTo(DeserializationHandlerResponse.FAIL);
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

	public static class Recoverer implements ConsumerRecordRecoverer {

		@Override
		public void accept(ConsumerRecord<?, ?> record, Exception exception) {
			if (exception instanceof IllegalStateException) {
				throw new IllegalStateException("recovery failed test");
			}
		}

	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfig {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Value("${streaming.topic.two}")
		private String streamingTopic2;

		@Bean
		public ProducerFactory<byte[], byte[]> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			Map<String, Object> senderProps = KafkaTestUtils.senderProps(this.brokerAddresses);
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
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, FailSerde.class);
			props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
					WallclockTimestampExtractor.class.getName());
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					RecoveringDeserializationExceptionHandler.class);
			props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());
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
					"false");
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
		public SettableListenableFuture<ConsumerRecord<byte[], byte[]>> resultFuture() {
			return new SettableListenableFuture<>();
		}

		@KafkaListener(topics = "recovererDLQ")
		public void listener(ConsumerRecord<byte[], byte[]> payload) {
			resultFuture().set(payload);
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
