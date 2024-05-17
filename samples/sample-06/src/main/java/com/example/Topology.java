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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Suppressed;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;


/**
 * A basic topology that counts records by key and materialises the output into a new topic
 *
 * @author Nacho Munoz
 * @author Soby Chacko
 * @since 3.2.0
 */
@Configuration
@Component
public class Topology {
	private final String inputTopic;

	private final String outputTopic;

	@Autowired
	public Topology(@Value("${input-topic.name}") final String inputTopic,
					@Value("${output-topic.name}") final String outputTopic) {
		this.inputTopic = inputTopic;
		this.outputTopic = outputTopic;
	}

	@Autowired
	public void defaultTopology(final StreamsBuilder builder) {
		builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
				.groupByKey()
				.count()
				.suppress(Suppressed.untilTimeLimit(Duration.ofMillis(5), Suppressed.BufferConfig.unbounded()))
				.toStream()
				.to(outputTopic);

	}

}
