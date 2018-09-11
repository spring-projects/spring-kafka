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

package org.springframework.kafka.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Composite {@link KafkaStreamsCustomizer} customizes {@link KafkaStreams} by delegating
 * to a list of provided {@link KafkaStreamsCustomizer}.
 *
 * @author Nurettin Yilmaz
 * @author Artem Bilan
 *
 * @since 2.1.5
 */
public class CompositeKafkaStreamsCustomizer implements KafkaStreamsCustomizer {

	private final List<KafkaStreamsCustomizer> kafkaStreamsCustomizers = new ArrayList<>();

	public CompositeKafkaStreamsCustomizer() {
	}

	public CompositeKafkaStreamsCustomizer(List<KafkaStreamsCustomizer> kafkaStreamsCustomizers) {
		this.kafkaStreamsCustomizers.addAll(kafkaStreamsCustomizers);
	}

	@Override
	public void customize(KafkaStreams kafkaStreams) {
		this.kafkaStreamsCustomizers.forEach(kafkaStreamsCustomizer -> kafkaStreamsCustomizer.customize(kafkaStreams));
	}

	public void addKafkaStreamsCustomizers(List<KafkaStreamsCustomizer> kafkaStreamsCustomizers) {
		this.kafkaStreamsCustomizers.addAll(kafkaStreamsCustomizers);
	}

}
