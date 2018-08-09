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

package org.springframework.kafka.kstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;
import org.junit.runner.RunWith;

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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Elliot Kennedy
 * @author Artem Bilan
 *
 * @since 1.3.3
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1,
		topics = {
				KafkaStreamsBranchTests.TRUE_TOPIC,
				KafkaStreamsBranchTests.FALSE_TOPIC,
				KafkaStreamsBranchTests.TRUE_FALSE_INPUT_TOPIC })
public class KafkaStreamsBranchTests {

	public static final String TRUE_TOPIC = "true-output-topic";

	public static final String FALSE_TOPIC = "false-output-topic";

	public static final String TRUE_FALSE_INPUT_TOPIC = "input-topic";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Test
	public void testBranchingStream() throws Exception {
		Consumer<String, String> falseConsumer = createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(falseConsumer, FALSE_TOPIC);

		Consumer<String, String> trueConsumer = createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(trueConsumer, TRUE_TOPIC);

		this.kafkaTemplate.sendDefault(String.valueOf(true));
		this.kafkaTemplate.sendDefault(String.valueOf(true));
		this.kafkaTemplate.sendDefault(String.valueOf(false));

		ConsumerRecords<String, String> trueRecords = KafkaTestUtils.getRecords(trueConsumer);
		ConsumerRecords<String, String> falseRecords = KafkaTestUtils.getRecords(falseConsumer);

		List<String> trueValues = new ArrayList<>();
		trueRecords.forEach(trueRecord -> trueValues.add(trueRecord.value()));

		List<String> falseValues = new ArrayList<>();
		falseRecords.forEach(falseRecord -> falseValues.add(falseRecord.value()));

		assertThat(trueValues).containsExactly("true", "true");
		assertThat(falseValues).containsExactly("false");

		falseConsumer.close();
		trueConsumer.close();
	}

	private Consumer<String, String> createConsumer() {
		Map<String, Object> consumerProps =
				KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false", this.embeddedKafka);
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);

		DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory =
				new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
		return kafkaConsumerFactory.createConsumer();
	}

	@Configuration
	@EnableKafkaStreams
	public static class Config {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.senderProps(this.brokerAddresses);
		}

		@Bean
		public KafkaTemplate<?, ?> kafkaTemplate() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			kafkaTemplate.setDefaultTopic(TRUE_FALSE_INPUT_TOPIC);
			return kafkaTemplate;
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			return new KafkaStreamsConfiguration(props);
		}

		@Bean
		@SuppressWarnings("unchecked")
		public KStream<String, String> trueFalseStream(StreamsBuilder streamsBuilder) {
			KStream<String, String> trueFalseStream = streamsBuilder
					.stream(TRUE_FALSE_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

			KStream<String, String>[] branches =
					trueFalseStream.branch((key, value) -> String.valueOf(true).equals(value),
							(key, value) -> String.valueOf(false).equals(value));

			branches[0].to(TRUE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
			branches[1].to(FALSE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

			return trueFalseStream;
		}

	}

}
