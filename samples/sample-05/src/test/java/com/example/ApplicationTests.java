/*
 * Copyright 2018-2021 the original author or authors.
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

package com.example;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EnableKafkaStreams
@TestPropertySource(properties = "spring.profiles.active=test")
@Import({TopologyTestDriverConfiguration.class})
public class ApplicationTests {
	private final Logger logger = LoggerFactory.getLogger(ApplicationTests.class);
	private TopologyTestDriver testDriver;
	@Value("${input-topic.name}")
	private String inputTopicName;
	@Value("${output-topic.name}")
	private String outputTopicName;

	private TestInputTopic inputTopic;
	private TestOutputTopic outputTopic;

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilder;

	@BeforeEach
	public void setup() {
		this.testDriver = new TopologyTestDriver(streamsBuilder.getTopology(), streamsBuilder.getStreamsConfiguration());
		logger.info(streamsBuilder.getTopology().describe().toString());
		this.inputTopic = testDriver.createInputTopic(inputTopicName, Serdes.Integer().serializer(), Serdes.String().serializer());
		this.outputTopic = testDriver.createOutputTopic(outputTopicName, Serdes.Integer().deserializer(), Serdes.Long().deserializer());
	}

	@Test
	public void testTopologyLogic() {
		inputTopic.pipeInput(1, "test", 1L);
		inputTopic.pipeInput(1, "test", 10L);
		inputTopic.pipeInput(2, "test", 2L);

		testDriver.advanceWallClockTime(Duration.ofMillis(100));

		assertThat(outputTopic.getQueueSize()).isEqualTo(2);
		assertThat(outputTopic.readValuesToList()).isEqualTo(List.of(2L, 1L));
	}
}

@Configuration
class TopologyTestDriverConfiguration {
	@Bean
	public StreamsBuilderFactoryBeanCustomizer testDriverCustomizer() {

		return fb -> fb.setClientSupplier(new KafkaClientSupplier() {
			@Override
			public Admin getAdmin(final Map<String, Object> config) {
				return new MockAdminClient(List.of(Node.noNode()), Node.noNode());
			}

			@Override
			public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
				return new MockProducer<>();
			}

			@Override
			public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
				return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
			}

			@Override
			public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
				return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
			}

			@Override
			public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
				return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
			}
		});
	}
}

