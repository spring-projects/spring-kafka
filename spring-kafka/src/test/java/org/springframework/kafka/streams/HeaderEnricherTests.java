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
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class HeaderEnricherTests {

	private static final String INPUT = "input";

	private static final String OUTPUT = "output";

	@Test
	void testWithDriver() {
		StreamsBuilder builder = new StreamsBuilder();
		Map<String, Expression> headers = new HashMap<>();
		headers.put("foo", new LiteralExpression("bar"));
		SpelExpressionParser parser = new SpelExpressionParser();
		headers.put("spel", parser.parseExpression("context.timestamp() + new String(key) + new String(value)"));
		HeaderEnricher<String, String> enricher = new HeaderEnricher<>(headers);
		KStream<String, String> stream = builder.stream(INPUT);
		stream
				.transform(() -> enricher)
				.to(OUTPUT);

		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
		TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config);

		TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT, new StringSerializer(),
				new StringSerializer());
		inputTopic.pipeInput("key", "value");
		TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(),
				new StringDeserializer());
		TestRecord<String, String> result = outputTopic.readRecord();
		assertThat(result.headers().lastHeader("foo")).isNotNull();
		assertThat(result.headers().lastHeader("foo").value()).isEqualTo("bar".getBytes());
		assertThat(result.headers().lastHeader("spel")).isNotNull();
		driver.close();
	}

}
