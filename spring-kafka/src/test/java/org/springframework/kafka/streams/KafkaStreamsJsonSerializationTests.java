/*
 * Copyright 2018-present the original author or authors.
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

import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Elliot Kennedy
 * @author Artem Bilan
 * @author Sanghyeok An
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1,
		topics = {
				KafkaStreamsJsonSerializationTests.OBJECT_INPUT_TOPIC,
				KafkaStreamsJsonSerializationTests.OBJECT_OUTPUT_TOPIC
		})
public class KafkaStreamsJsonSerializationTests {

	public static final String OBJECT_INPUT_TOPIC = "object-input-topic";

	public static final String OBJECT_OUTPUT_TOPIC = "object-output-topic";

	public static final JacksonJsonSerde<JsonObjectKey> jsonObjectKeySerde =
			new JacksonJsonSerde<>(JsonObjectKey.class).forKeys();

	public static final JacksonJsonSerde<JsonObjectValue> jsonObjectValueSerde = new JacksonJsonSerde<>(JsonObjectValue.class);

	@Autowired
	private KafkaTemplate<Object, Object> template;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	private Consumer<JsonObjectKey, JsonObjectValue> objectOutputTopicConsumer;

	@BeforeEach
	public void setup() {
		this.objectOutputTopicConsumer = consumer(OBJECT_OUTPUT_TOPIC, jsonObjectKeySerde, jsonObjectValueSerde);
	}

	@AfterEach
	public void teardown() {
		if (this.objectOutputTopicConsumer != null) {
			this.objectOutputTopicConsumer.close();
		}
	}

	@Test
	public void testJsonObjectSerialization() {
		template.send(OBJECT_INPUT_TOPIC, new JsonObjectKey(25), new JsonObjectValue("twenty-five"));

		ConsumerRecords<JsonObjectKey, JsonObjectValue> outputTopicRecords =
				KafkaTestUtils.getRecords(this.objectOutputTopicConsumer);

		assertThat(outputTopicRecords.count()).isEqualTo(1);
		ConsumerRecord<JsonObjectKey, JsonObjectValue> output = outputTopicRecords.iterator().next();
		assertThat(output.key()).isInstanceOf(JsonObjectKey.class);
		assertThat(output.key().getKey()).isEqualTo(25);
		assertThat(output.value()).isInstanceOf(JsonObjectValue.class);
		assertThat(output.value().getValue()).isEqualTo("twenty-five");
	}

	private <K, V> Consumer<K, V> consumer(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
		Map<String, Object> consumerProps =
				KafkaTestUtils.consumerProps(this.embeddedKafka, UUID.randomUUID().toString(), false);
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);

		DefaultKafkaConsumerFactory<K, V> kafkaConsumerFactory =
				new DefaultKafkaConsumerFactory<>(consumerProps, keySerde.deserializer(), valueSerde.deserializer());
		Consumer<K, V> consumer = kafkaConsumerFactory.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic);
		return consumer;
	}

	public static class JsonObjectKey {

		private final Integer key;

		@JsonCreator
		public JsonObjectKey(@JsonProperty(value = "key", required = true) Integer key) {
			this.key = key;
		}

		public Integer getKey() {
			return key;
		}

		@Override
		public String toString() {
			return "JsonObjectKey{" +
					"key=" + key +
					'}';
		}

	}

	public static class JsonObjectValue {

		private final String value;

		@JsonCreator
		public JsonObjectValue(@JsonProperty(value = "value", required = true) String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public static Serde<JsonObjectValue> jsonObjectValueSerde() {
			return new JacksonJsonSerde<>(JsonObjectValue.class);
		}

		@Override
		public String toString() {
			return "JsonObjectValue{" +
					"value='" + value + '\'' +
					'}';
		}

	}

	@Configuration
	@EnableKafkaStreams
	public static class Config {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean
		public ProducerFactory<?, ?> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(this.brokerAddresses);
			senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
			senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
			return senderProps;
		}

		@Bean
		public KafkaTemplate<?, ?> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = KafkaTestUtils.streamsProps("testStreams", this.brokerAddresses);
			return new KafkaStreamsConfiguration(props);
		}

		@Bean
		public KStream<JsonObjectKey, JsonObjectValue> jsonObjectSerializationStream(StreamsBuilder streamsBuilder) {
			KStream<JsonObjectKey, JsonObjectValue> testStream = streamsBuilder
					.stream(OBJECT_INPUT_TOPIC, Consumed.with(jsonObjectKeySerde, jsonObjectValueSerde));

			testStream.print(Printed.toSysOut());
			testStream.to(OBJECT_OUTPUT_TOPIC, Produced.with(jsonObjectKeySerde, jsonObjectValueSerde));

			return testStream;
		}

	}

}
