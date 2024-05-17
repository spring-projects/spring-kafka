/*
 * Copyright 2024 the original author or authors.
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

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * @author Nacho Munoz
 * @author Soby Chacko
 * @since 3.2.0
 */
@SpringBootTest
public class ApplicationTests {
	private final Logger logger = LoggerFactory.getLogger(ApplicationTests.class);

	private TopologyTestDriver testDriver;

	@Value("${input-topic.name}")
	private String inputTopicName;

	@Value("${output-topic.name}")
	private String outputTopicName;

	private TestInputTopic<Integer,String> inputTopic;

	private TestOutputTopic<Integer, Long> outputTopic;

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilder;

	@BeforeEach
	public void setup() {
		this.testDriver = new TopologyTestDriver(streamsBuilder.getTopology(), streamsBuilder.getStreamsConfiguration());
		logger.info(streamsBuilder.getTopology().describe().toString());
		this.inputTopic = testDriver.createInputTopic(inputTopicName, Serdes.Integer().serializer(), Serdes.String().serializer());
		this.outputTopic = testDriver.createOutputTopic(outputTopicName, Serdes.Integer().deserializer(), Serdes.Long().deserializer());
	}

	@AfterEach
	public void after() {
		if (testDriver != null) {
			this.testDriver.close();
		}
	}

	@Test
	public void testTopologyLogic() {
		inputTopic.pipeInput(1, "test", 1L);
		inputTopic.pipeInput(1, "test", 10L);
		inputTopic.pipeInput(2, "test", 2L);

		Awaitility.waitAtMost(Duration.ofSeconds(5)).until(() -> outputTopic.getQueueSize() == 2L);
		assertThat(outputTopic.readValuesToList()).isEqualTo(List.of(2L, 1L));
	}

}
