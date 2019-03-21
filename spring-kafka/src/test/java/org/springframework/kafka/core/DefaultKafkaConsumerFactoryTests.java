/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

/**
 * @author Gary Russell
 * @since 1.0.6
 *
 */
public class DefaultKafkaConsumerFactoryTests {

	@Test
	public void testClientId() {
		Map<String, Object> configs = Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "foo");
		DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<String, String>(configs) {

			@Override
			protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configs) {
				assertThat(configs.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("foo-1");
				return null;
			}

		};
		factory.createConsumer("-1");
	}

}
