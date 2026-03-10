/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DefaultShareConsumerRecordRecoverer}.
 *
 * @author Soby Chacko
 * @since 4.1
 */
class DefaultShareConsumerRecordRecovererTests {

	private final DefaultShareConsumerRecordRecoverer recoverer = new DefaultShareConsumerRecordRecoverer();

	@Test
	void recoverAlwaysReturnsReject() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 1, 42L, "key", "value");
		assertThat(recoverer.recover(record, new RuntimeException("test"))).isEqualTo(AcknowledgeType.REJECT);
	}

}
