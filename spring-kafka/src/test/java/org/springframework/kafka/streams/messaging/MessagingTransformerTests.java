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

package org.springframework.kafka.streams.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class MessagingTransformerTests {

	private static final String INPUT = "input";

	private static final String OUTPUT = "output";

	@Test
	void testWithDriver() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		converter.setHeaderMapper(new SimpleKafkaHeaderMapper("*"));
		MessagingTransformer<String, String, String> messagingTransformer = new MessagingTransformer<>(message ->
					MessageBuilder.withPayload("bar".getBytes())
						.copyHeaders(message.getHeaders())
						.setHeader("baz", "qux".getBytes())
						.build(),
				converter);
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> stream = builder.stream(INPUT);
		stream
				.transform(() -> messagingTransformer)
				.to(OUTPUT);

		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
		TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config);

		TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT, new StringSerializer(),
				new StringSerializer());
		Headers headers = new RecordHeaders(Collections.singletonList(new RecordHeader("fiz", "buz".getBytes())));
		inputTopic.pipeInput(new TestRecord<>("key", "value", headers));
		TestOutputTopic<byte[], byte[]> outputTopic = driver.createOutputTopic(OUTPUT, new ByteArrayDeserializer(),
				new ByteArrayDeserializer());
		TestRecord<byte[], byte[]> result = outputTopic.readRecord();
		assertThat(result.value()).isEqualTo("bar".getBytes());
		assertThat(result.headers().lastHeader("fiz").value()).isEqualTo("buz".getBytes());
		assertThat(result.headers().lastHeader("baz").value()).isEqualTo("qux".getBytes());
		driver.close();
	}

}
