/*
 * Copyright 2020-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaOperations.OperationsCallback;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * @author Gary Russell
 * @since 2.4.3
 *
 */
public class DeadLetterPublishingRecovererTests {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testTxNoTx() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(false);
		given(template.isAllowNonTransactional()).willReturn(true);
		given(template.send(any(ProducerRecord.class))).willReturn(new SettableListenableFuture());
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		recoverer.accept(record, new RuntimeException());
		verify(template, never()).executeInTransaction(any());
		verify(template).send(any(ProducerRecord.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testTxExisting() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(true);
		given(template.send(any(ProducerRecord.class))).willReturn(new SettableListenableFuture());
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		recoverer.accept(record, new RuntimeException());
		verify(template, never()).executeInTransaction(any());
		verify(template).send(any(ProducerRecord.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testNonTx() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(false);
		given(template.send(any(ProducerRecord.class))).willReturn(new SettableListenableFuture());
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
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(false);
		given(template.isAllowNonTransactional()).willReturn(false);
		willAnswer(inv -> {
			((OperationsCallback) inv.getArgument(0)).doInOperations(template);
			return null;
		}).given(template).executeInTransaction(any());
		given(template.send(any(ProducerRecord.class))).willReturn(new SettableListenableFuture());
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		recoverer.accept(record, new RuntimeException());
		verify(template).executeInTransaction(any());
		verify(template).send(any(ProducerRecord.class));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void valueHeaderStripped() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER, header(false)));
		headers.add(new RecordHeader(ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER, header(true)));
		Headers custom = new RecordHeaders();
		custom.add(new RecordHeader("foo", "bar".getBytes()));
		recoverer.setHeadersFunction((rec, ex) -> custom);
		willReturn(new SettableListenableFuture<Object>()).given(template).send(any(ProducerRecord.class));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.CREATE_TIME,
				0L, 0, 0, "bar", "baz", headers);
		recoverer.accept(record, new RuntimeException("testV"));
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		ProducerRecord recovered = captor.getValue();
		assertThat(recovered.key()).isEqualTo("key".getBytes());
		assertThat(recovered.value()).isEqualTo("value".getBytes());
		headers = recovered.headers();
		assertThat(headers.lastHeader(ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER)).isNull();
		assertThat(headers.lastHeader(ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER)).isNull();
		assertThat(headers.lastHeader("foo")).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE).value()).isEqualTo("testK".getBytes());
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value()).isEqualTo("testV".getBytes());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void keyHeaderStripped() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER, header(true)));
		willReturn(new SettableListenableFuture<Object>()).given(template).send(any(ProducerRecord.class));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.CREATE_TIME,
				0L, 0, 0, "bar", "baz", headers);
		recoverer.accept(record, new RuntimeException());
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		headers = captor.getValue().headers();
		assertThat(headers.lastHeader(ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER)).isNull();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void headersNotStripped() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setRetainExceptionHeader(true);
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER, header(false)));
		headers.add(new RecordHeader(ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER, header(true)));
		willReturn(new SettableListenableFuture<Object>()).given(template).send(any(ProducerRecord.class));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.CREATE_TIME,
				0L, 0, 0, "bar", "baz", headers);
		recoverer.accept(record, new RuntimeException());
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		headers = captor.getValue().headers();
		assertThat(headers.lastHeader(ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER)).isNotNull();
		assertThat(headers.lastHeader(ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER)).isNotNull();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void tombstoneWithMultiTemplates() {
		KafkaOperations<?, ?> template1 = mock(KafkaOperations.class);
		given(template1.send(any(ProducerRecord.class))).willReturn(new SettableListenableFuture());
		KafkaOperations<?, ?> template2 = mock(KafkaOperations.class);
		Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
		templates.put(String.class, template1);
		templates.put(Integer.class, template2);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		recoverer.accept(record, new RuntimeException());
		verify(template1).send(any(ProducerRecord.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void tombstoneWithMultiTemplatesExplicit() {
		KafkaOperations<?, ?> template1 = mock(KafkaOperations.class);
		KafkaOperations<?, ?> template2 = mock(KafkaOperations.class);
		given(template2.send(any(ProducerRecord.class))).willReturn(new SettableListenableFuture());
		Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
		templates.put(String.class, template1);
		templates.put(Void.class, template2);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		recoverer.accept(record, new RuntimeException());
		verify(template2).send(any(ProducerRecord.class));
	}

	private byte[] header(boolean isKey) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			new ObjectOutputStream(baos).writeObject(new DeserializationException(
					isKey ? "testK" : "testV",
					isKey ? "key".getBytes() : "value".getBytes(), isKey, null));
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return baos.toByteArray();
	}
}
