/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.config;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.Topology;

/**
 * Composite {@link KafkaStreamsTopologyCustomizer} customizes {@link Topology} by delegating
 * to a list of provided {@link KafkaStreamsTopologyCustomizer}.
 *
 * @author Renato Mefi
 *
 * @since 2.4.0
 */
public class CompositeKafkaStreamsTopologyCustomizer implements KafkaStreamsTopologyCustomizer {

	private final List<KafkaStreamsTopologyCustomizer> KafkaStreamsTopologyCustomizers = new ArrayList<>();

	public CompositeKafkaStreamsTopologyCustomizer() {
	}

	public CompositeKafkaStreamsTopologyCustomizer(List<KafkaStreamsTopologyCustomizer> KafkaStreamsTopologyCustomizers) {
		this.KafkaStreamsTopologyCustomizers.addAll(KafkaStreamsTopologyCustomizers);
	}

	@Override
	public void customize(Topology topology) {
		this.KafkaStreamsTopologyCustomizers.forEach(KafkaStreamsTopologyCustomizer -> KafkaStreamsTopologyCustomizer.customize(topology));
	}

	public void addKafkaStreamsTopologyCustomizers(List<KafkaStreamsTopologyCustomizer> customizers) {
		this.KafkaStreamsTopologyCustomizers.addAll(customizers);
	}

}
