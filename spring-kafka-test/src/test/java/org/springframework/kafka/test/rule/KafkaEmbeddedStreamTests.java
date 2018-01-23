/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test.rule;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Elliot Kennedy
 */
@RunWith(SpringRunner.class)
public class KafkaEmbeddedStreamTests {

	private static final String TRUE_TOPIC = "true-output-topic";
	private static final String FALSE_TOPIC = "false-output-topic";
	private static final String TRUE_FALSE_INPUT_TOPIC = "input-topic";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, false, TRUE_FALSE_INPUT_TOPIC, FALSE_TOPIC, TRUE_TOPIC);

	private KafkaTemplate<String, String> kafkaTemplate;

	@Before
	public void setup() throws Exception {
		kafkaTemplate = createKafkaTemplate();
	}

	@Test
	public void testConsumeFromAnEmbeddedTopic() throws Exception {
		Consumer<String, String> falseConsumer = createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(falseConsumer, FALSE_TOPIC);

		Consumer<String, String> trueConsumer = createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(trueConsumer, TRUE_TOPIC);

		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(true));
		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(true));
		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(false));

		List<String> trueMessages = consumeAll(trueConsumer);
		List<String> falseMessages = consumeAll(falseConsumer);

		assertThat(trueMessages).containsExactlyInAnyOrder("true", "true");
		assertThat(falseMessages).containsExactlyInAnyOrder("false");
	}

	@Test
	public void testConsumeFromEmbeddedTopics() throws Exception {
		Consumer<String, String> trueAndFalseConsumer = createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(trueAndFalseConsumer, FALSE_TOPIC, TRUE_TOPIC);

		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(true));
		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(true));
		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(false));

		List<String> trueAndFalseMessages = consumeAll(trueAndFalseConsumer);

		assertThat(trueAndFalseMessages).containsExactlyInAnyOrder("true", "true", "false");
	}

	@Test
	public void testConsumeFromAllEmbeddedTopics() throws Exception {
		Consumer<String, String> consumeFromAllTopicsConsumer = createConsumer();
		embeddedKafka.consumeFromAllEmbeddedTopics(consumeFromAllTopicsConsumer);

		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(true));
		kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(false));

		List<String> allMessages = consumeAll(consumeFromAllTopicsConsumer);

		assertThat(allMessages).containsExactlyInAnyOrder("true", "false", "true", "false");
	}

	private List<String> consumeAll(Consumer<String, String> consumer) {
		List<String> allMessages = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			ConsumerRecords<String, String> records = consumer.poll(10L);
			records.forEach(record -> allMessages.add(record.value()));
		}
		return allMessages;
	}

	private Consumer<String, String> createConsumer() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);

		DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
		return kafkaConsumerFactory.createConsumer();
	}

	private KafkaTemplate<String, String> createKafkaTemplate() {
		Map<String, Object> senderProperties = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
		senderProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		senderProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(senderProperties);
		return new KafkaTemplate<>(producerFactory);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Value("${spring.embedded.kafka.brokers}")
		private String brokers;

		@Bean
		public FactoryBean<StreamsBuilder> streamsBuilder() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0L);
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
			return new StreamsBuilderFactoryBean(new StreamsConfig(props));
		}

		@Bean
		public KStream<String, String> trueFalseStream(StreamsBuilder streamsBuilder) {
			KStream<String, String> trueFalseStream = streamsBuilder
					.stream(TRUE_FALSE_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

			KStream<String, String>[] branches = trueFalseStream.branch(
					(key, value) -> String.valueOf(true).equals(value),
					(key, value) -> String.valueOf(false).equals(value));

			branches[0].to(TRUE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
			branches[1].to(FALSE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

			return trueFalseStream;
		}
	}

}
