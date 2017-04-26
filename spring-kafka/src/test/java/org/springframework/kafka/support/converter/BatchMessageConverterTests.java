/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.support.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Biju Kunjummen
 * @since 2.0
 */
public class BatchMessageConverterTests {

	@Test
	public void testBatchConverters() throws Exception {
		List<ConsumerRecord<?, ?>> consumerRecords = new ArrayList<>();
		consumerRecords.add(new ConsumerRecord<>("topic1", 0, 1, 1487694048607L,
				TimestampType.CREATE_TIME, 123L, 2, 3, "key1", "value1"));
		consumerRecords.add(new ConsumerRecord<>("topic1", 0, 2, 1487694048608L,
				TimestampType.CREATE_TIME, 123L, 2, 3, "key2", "value2"));
		consumerRecords.add(new ConsumerRecord<>("topic1", 0, 3, 1487694048609L,
				TimestampType.CREATE_TIME, 123L, 2, 3, "key3", "value3"));

		BatchMessageConverter batchMessageConverter = new BatchMessagingMessageConverter();

		Acknowledgment ack = mock(Acknowledgment.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		Message<?> message = batchMessageConverter.toMessage(consumerRecords, ack, consumer,
				String.class);

		assertThat(message.getPayload())
				.isEqualTo(Arrays.asList("value1", "value2", "value3"));

		MessageHeaders headers = message.getHeaders();
		assertThat(headers.get(KafkaHeaders.RECEIVED_TOPIC))
				.isEqualTo(Arrays.asList("topic1", "topic1", "topic1"));
		assertThat(headers.get(KafkaHeaders.RECEIVED_MESSAGE_KEY))
				.isEqualTo(Arrays.asList("key1", "key2", "key3"));
		assertThat(headers.get(KafkaHeaders.RECEIVED_PARTITION_ID))
				.isEqualTo(Arrays.asList(0, 0, 0));
		assertThat(headers.get(KafkaHeaders.OFFSET)).isEqualTo(Arrays.asList(1L, 2L, 3L));
		assertThat(headers.get(KafkaHeaders.TIMESTAMP_TYPE))
				.isEqualTo(Arrays.asList("CREATE_TIME", "CREATE_TIME", "CREATE_TIME"));
		assertThat(headers.get(KafkaHeaders.RECEIVED_TIMESTAMP))
				.isEqualTo(Arrays.asList(1487694048607L, 1487694048608L, 1487694048609L));
		assertThat(headers.get(KafkaHeaders.ACKNOWLEDGMENT)).isSameAs(ack);
		assertThat(headers.get(KafkaHeaders.CONSUMER)).isSameAs(consumer);
	}

}
