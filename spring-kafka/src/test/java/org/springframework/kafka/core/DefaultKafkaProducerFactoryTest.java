/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

/**
 * @author Nakul Mishra
 */

public class DefaultKafkaProducerFactoryTest {

	@Test
	public void testDefaultProducerIdempotentConfig() throws Exception {
		Map<String, Object> senderProps = new HashMap<>();
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
		pf.setTransactionIdPrefix("foo.");
		pf.destroy();
		assertThat(pf.getConfigurationProperties()
				.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
	}

	@Test
	public void testOverrideProducerIdempotentConfig() throws Exception {
		Map<String, Object> senderProps = new HashMap<>();
		senderProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
		pf.setTransactionIdPrefix("foo.");
		pf.destroy();
		assertThat(pf.getConfigurationProperties()
				.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(false);
	}
}
