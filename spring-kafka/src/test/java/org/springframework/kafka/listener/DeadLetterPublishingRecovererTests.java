/*
 * Copyright 2020 the original author or authors.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.KafkaOperations.OperationsCallback;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author Gary Russell
 * @since 2.4.3
 *
 */
public class DeadLetterPublishingRecovererTests {

	@SuppressWarnings("unchecked")
	@Test
	void testTxNoTx() {
		KafkaTemplate<?, ?> template = mock(KafkaTemplate.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(false);
		given(template.isAllowNonTransactional()).willReturn(true);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		recoverer.accept(record, new RuntimeException());
		verify(template, never()).executeInTransaction(any());
		verify(template).send(any(ProducerRecord.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	void testTxExisting() {
		KafkaTemplate<?, ?> template = mock(KafkaTemplate.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(true);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		recoverer.accept(record, new RuntimeException());
		verify(template, never()).executeInTransaction(any());
		verify(template).send(any(ProducerRecord.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	void testNonTx() {
		KafkaTemplate<?, ?> template = mock(KafkaTemplate.class);
		given(template.isTransactional()).willReturn(false);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		recoverer.accept(record, new RuntimeException());
		verify(template, never()).inTransaction();
		verify(template, never()).executeInTransaction(any());
		verify(template).send(any(ProducerRecord.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testTxNewTx() {
		KafkaTemplate<?, ?> template = mock(KafkaTemplate.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(false);
		given(template.isAllowNonTransactional()).willReturn(false);
		willAnswer(inv -> {
			((OperationsCallback) inv.getArgument(0)).doInOperations(template);
			return null;
		}).given(template).executeInTransaction(any());
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		recoverer.accept(record, new RuntimeException());
		verify(template).executeInTransaction(any());
		verify(template).send(any(ProducerRecord.class));
	}

}
