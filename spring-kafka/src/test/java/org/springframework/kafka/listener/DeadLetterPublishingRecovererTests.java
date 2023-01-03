/*
 * Copyright 2020-2023 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willCallRealMethod;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaOperations.OperationsCallback;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.SingleRecordHeader;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @author Tomaz Fernandes
 * @since 2.4.3
 *
 */
@SuppressWarnings("deprecation")
public class DeadLetterPublishingRecovererTests {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testTxNoTx() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(false);
		given(template.isAllowNonTransactional()).willReturn(true);
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		Consumer consumer = mock(Consumer.class);
		given(consumer.partitionsFor("foo.DLT", Duration.ofSeconds(5)))
				.willReturn(Collections.singletonList(new PartitionInfo("foo", 0, null, null, null)));
		recoverer.accept(record, consumer, new RuntimeException());
		verify(template, never()).executeInTransaction(any());
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		assertThat(captor.getValue().partition()).isEqualTo(0);
		verify(consumer).partitionsFor("foo.DLT", Duration.ofSeconds(5));

		record = new ConsumerRecord<>("foo", 1, 0L, "bar", "baz");
		recoverer.accept(record, consumer, new RuntimeException());
		verify(template, times(2)).send(captor.capture());
		assertThat(captor.getValue().partition()).isNull();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testTxExisting() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(true);
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		given(template.send(any(ProducerRecord.class))).willReturn(future);
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
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		given(template.send(any(ProducerRecord.class))).willReturn(future);
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
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		given(template.send(any(ProducerRecord.class))).willReturn(future);
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
		headers.add(new RecordHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, header(false)));
		headers.add(new RecordHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, header(true)));
		Headers custom = new RecordHeaders();
		custom.add(new RecordHeader("foo", "bar".getBytes()));
		recoverer.setHeadersFunction((rec, ex) -> custom);
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		willReturn(future).given(template).send(any(ProducerRecord.class));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.CREATE_TIME,
				0, 0, "bar", "baz", headers, Optional.empty());
		recoverer.accept(record, new RuntimeException("testV"));
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		ProducerRecord recovered = captor.getValue();
		assertThat(recovered.key()).isEqualTo("key".getBytes());
		assertThat(recovered.value()).isEqualTo("value".getBytes());
		headers = recovered.headers();
		assertThat(headers.lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)).isNull();
		assertThat(headers.lastHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER)).isNull();
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
		headers.add(new RecordHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, header(true)));
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		willReturn(future).given(template).send(any(ProducerRecord.class));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.CREATE_TIME,
				0, 0, "bar", "baz", headers, Optional.empty());
		recoverer.accept(record, new RuntimeException());
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		headers = captor.getValue().headers();
		assertThat(headers.lastHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER)).isNull();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void keyDeserOnly() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		Headers headers = new RecordHeaders();
		DeserializationException deserEx = createDeserEx(true);
		headers.add(
				new RecordHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, header(true, deserEx)));
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		willReturn(future).given(template).send(any(ProducerRecord.class));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.CREATE_TIME,
				0, 0, "bar", "baz", headers, Optional.empty());
		recoverer.accept(record, deserEx);
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		headers = captor.getValue().headers();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE).value()).isEqualTo("testK".getBytes());
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNull();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void headersNotStripped() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setRetainExceptionHeader(true);
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, header(false)));
		headers.add(new RecordHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, header(true)));
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		willReturn(future).given(template).send(any(ProducerRecord.class));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.CREATE_TIME,
				0, 0, "bar", "baz", headers, Optional.empty());
		recoverer.accept(record, new RuntimeException());
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		headers = captor.getValue().headers();
		assertThat(headers.lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)).isNotNull();
		assertThat(headers.lastHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE).value()).isEqualTo("testK".getBytes());
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value()).isEqualTo("testV".getBytes());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void tombstoneWithMultiTemplates() {
		KafkaOperations<?, ?> template1 = mock(KafkaOperations.class);
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		given(template1.send(any(ProducerRecord.class))).willReturn(future);
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
		CompletableFuture future = new CompletableFuture();
		future.complete(new Object());
		given(template2.send(any(ProducerRecord.class))).willReturn(future);
		Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
		templates.put(String.class, template1);
		templates.put(Void.class, template2);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		recoverer.accept(record, new RuntimeException());
		verify(template2).send(any(ProducerRecord.class));
	}

	private byte[] header(boolean isKey) {
		return header(isKey, createDeserEx(isKey));
	}

	private DeserializationException createDeserEx(boolean isKey) {
		return new DeserializationException(
				isKey ? "testK" : "testV",
				isKey ? "key".getBytes() : "value".getBytes(), isKey, null);
	}

	private byte[] header(boolean isKey, DeserializationException deserEx) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			new ObjectOutputStream(baos).writeObject(deserEx);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return baos.toByteArray();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	void allOriginalHeaders() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.accept(record, new RuntimeException());
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(producerRecordCaptor.capture());
		ProducerRecord outRecord = producerRecordCaptor.getValue();
		Headers headers = outRecord.headers();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	void dontAppendOriginalHeaders() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 1234L,
				TimestampType.CREATE_TIME, 123, 123, "bar", null, new RecordHeaders(), Optional.empty());
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setAppendOriginalHeaders(false);
		recoverer.accept(record, new RuntimeException(new IllegalStateException()));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		then(template).should(times(1)).send(producerRecordCaptor.capture());
		Headers headers = producerRecordCaptor.getValue().headers();
		Header originalTopicHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC);
		Header originalPartitionHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION);
		Header originalOffsetHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET);
		Header originalTimestampHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP);
		Header originalTimestampType = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE);
		Header firstExceptionType = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN);
		Header firstExceptionCauseType = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
		Header firstExceptionMessage = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
		Header firstExceptionStackTrace = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE);

		ConsumerRecord<String, String> anotherRecord = new ConsumerRecord<>("bar", 1, 12L, 4321L,
				TimestampType.LOG_APPEND_TIME, 321, 321, "bar", null, new RecordHeaders(), Optional.empty());
		headers.forEach(header -> anotherRecord.headers().add(header));
		recoverer.accept(anotherRecord, new RuntimeException(new IllegalStateException()));
		ArgumentCaptor<ProducerRecord> anotherProducerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		then(template).should(times(2)).send(anotherProducerRecordCaptor.capture());
		Headers anotherHeaders = anotherProducerRecordCaptor.getAllValues().get(1).headers();
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isSameAs(originalTopicHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isSameAs(originalPartitionHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isSameAs(originalOffsetHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isSameAs(originalTimestampHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE))
				.isSameAs(originalTimestampType);
		Iterator<Header> originalTopics = anotherHeaders.headers(KafkaHeaders.DLT_ORIGINAL_TOPIC).iterator();
		assertThat(originalTopics.next()).isSameAs(originalTopicHeader);
		assertThat(originalTopics.hasNext()).isFalse();
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotSameAs(firstExceptionType);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
				.isNotSameAs(firstExceptionCauseType);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotSameAs(firstExceptionMessage);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE))
				.isNotSameAs(firstExceptionStackTrace);
		Iterator<Header> exceptionHeaders = anotherHeaders.headers(KafkaHeaders.DLT_EXCEPTION_FQCN).iterator();
		assertThat(exceptionHeaders.next()).isNotSameAs(firstExceptionType);
		assertThat(exceptionHeaders.hasNext()).isFalse();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	void appendOriginalHeaders() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 1234L,
				TimestampType.CREATE_TIME, 123, 123, "bar", null, new RecordHeaders(), Optional.empty());
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setAppendOriginalHeaders(true);
		recoverer.setStripPreviousExceptionHeaders(false);
		recoverer.accept(record, new RuntimeException(new IllegalStateException()));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		then(template).should(times(1)).send(producerRecordCaptor.capture());
		Headers headers = producerRecordCaptor.getValue().headers();
		Header originalTopicHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC);
		Header originalPartitionHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION);
		Header originalOffsetHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET);
		Header originalTimestampHeader = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP);
		Header originalTimestampType = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE);
		Header firstExceptionType = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN);
		Header firstExceptionCauseType = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
		Header firstExceptionMessage = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
		Header firstExceptionStackTrace = headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE);

		ConsumerRecord<String, String> anotherRecord = new ConsumerRecord<>("bar", 1, 12L, 4321L,
				TimestampType.LOG_APPEND_TIME, 321, 321, "bar", null, new RecordHeaders(), Optional.empty());
		headers.forEach(header -> anotherRecord.headers().add(header));
		recoverer.accept(anotherRecord, new RuntimeException(new IllegalStateException()));
		ArgumentCaptor<ProducerRecord> anotherProducerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		then(template).should(times(2)).send(anotherProducerRecordCaptor.capture());
		Headers anotherHeaders = anotherProducerRecordCaptor.getAllValues().get(1).headers();
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotSameAs(originalTopicHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION))
				.isNotSameAs(originalPartitionHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotSameAs(originalOffsetHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP))
				.isNotSameAs(originalTimestampHeader);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE))
				.isNotSameAs(originalTimestampType);
		Iterator<Header> originalTopics = anotherHeaders.headers(KafkaHeaders.DLT_ORIGINAL_TOPIC).iterator();
		assertThat(originalTopics.next()).isSameAs(originalTopicHeader);
		assertThat(originalTopics.next()).isNotNull();
		assertThat(originalTopics.hasNext()).isFalse();
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotSameAs(firstExceptionType);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
				.isNotSameAs(firstExceptionCauseType);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotSameAs(firstExceptionMessage);
		assertThat(anotherHeaders.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE))
				.isNotSameAs(firstExceptionStackTrace);
		Iterator<Header> exceptionHeaders = anotherHeaders.headers(KafkaHeaders.DLT_EXCEPTION_FQCN).iterator();
		assertThat(exceptionHeaders.next()).isSameAs(firstExceptionType);
		assertThat(exceptionHeaders.next()).isNotNull();
		assertThat(exceptionHeaders.hasNext()).isFalse();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void failIfSendResultIsError() throws Exception {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10L);
		given(pf.getConfigurationProperties()).willReturn(props);
		given(template.getProducerFactory()).willReturn(pf);
		CompletableFuture<?> future = mock(CompletableFuture.class);
		ArgumentCaptor<Long> timeoutCaptor = ArgumentCaptor.forClass(Long.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		given(future.get(timeoutCaptor.capture(), eq(TimeUnit.MILLISECONDS))).willThrow(new TimeoutException());
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setFailIfSendResultIsError(true);
		Duration waitForSendResultTimeout = Duration.ofSeconds(1);
		recoverer.setWaitForSendResultTimeout(waitForSendResultTimeout);
		recoverer.setTimeoutBuffer(0L);
		assertThatThrownBy(() -> recoverer.accept(record, new RuntimeException()))
				.isExactlyInstanceOf(KafkaException.class);
		assertThat(timeoutCaptor.getValue()).isEqualTo(waitForSendResultTimeout.toMillis());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void sendTimeoutDefault() throws Exception {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		Map<String, Object> props = new HashMap<>();
		given(pf.getConfigurationProperties()).willReturn(props);
		given(template.getProducerFactory()).willReturn(pf);
		CompletableFuture<SendResult> future = spy(new CompletableFuture<>());
		ArgumentCaptor<Long> timeoutCaptor = ArgumentCaptor.forClass(Long.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		willAnswer(inv -> {
			future.complete(new SendResult(null, null));
			return null;
		}).given(future).get(timeoutCaptor.capture(), eq(TimeUnit.MILLISECONDS));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setFailIfSendResultIsError(true);
		Duration waitForSendResultTimeout = Duration.ofSeconds(1);
		recoverer.setWaitForSendResultTimeout(waitForSendResultTimeout);
		recoverer.accept(record, new RuntimeException());
		assertThat(timeoutCaptor.getValue()).isEqualTo(Duration.ofSeconds(125).toMillis());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void sendTimeoutConfig() throws Exception {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000L);
		given(pf.getConfigurationProperties()).willReturn(props);
		given(template.getProducerFactory()).willReturn(pf);
		CompletableFuture<SendResult> future = spy(new CompletableFuture<>());
		ArgumentCaptor<Long> timeoutCaptor = ArgumentCaptor.forClass(Long.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		willAnswer(inv -> {
			future.complete(new SendResult(null, null));
			return null;
		}).given(future).get(timeoutCaptor.capture(), eq(TimeUnit.MILLISECONDS));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setFailIfSendResultIsError(true);
		Duration waitForSendResultTimeout = Duration.ofSeconds(1);
		recoverer.setWaitForSendResultTimeout(waitForSendResultTimeout);
		recoverer.accept(record, new RuntimeException());
		assertThat(timeoutCaptor.getValue()).isEqualTo(Duration.ofSeconds(35).toMillis());
	}

	@SuppressWarnings("unchecked")
	@Test
	void notFailIfSendResultIsError() throws Exception {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture<?> future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		given(future.get(anyLong(), eq(TimeUnit.MILLISECONDS))).willThrow(new TimeoutException());
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setFailIfSendResultIsError(false);
		recoverer.accept(record, new RuntimeException());
	}

	@SuppressWarnings("unchecked")
	@Test
	void throwIfNoDestinationReturned() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture<?> future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (cr, e) -> null);
		recoverer.setThrowIfNoDestinationReturned(true);
		assertThatThrownBy(() -> recoverer.accept(record, new RuntimeException()))
				.isExactlyInstanceOf(IllegalArgumentException.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	void notThrowIfNoDestinationReturnedByDefault() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture<?> future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (cr, e) -> null);
		recoverer.accept(record, new RuntimeException());
	}

	@SuppressWarnings("unchecked")
	@Test
	void noCircularRoutingIfFatal() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture<Object> future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
				(cr, e) -> new TopicPartition("foo", 0));
		recoverer.accept(record, new ClassCastException());
		verify(template, never()).send(any(ProducerRecord.class));
		recoverer.addNotRetryableExceptions(IllegalStateException.class);
		recoverer.accept(record, new IllegalStateException());
		verify(template, never()).send(any(ProducerRecord.class));
		recoverer.removeClassification(IllegalStateException.class);
		recoverer.setFailIfSendResultIsError(false);
		recoverer.accept(record, new IllegalStateException());
		verify(template).send(any(ProducerRecord.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	void doNotSkipCircularFatalIfSet() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture<Object> future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
				(cr, e) -> new TopicPartition("foo", 0));
		recoverer.setSkipSameTopicFatalExceptions(false);
		recoverer.accept(record, new ClassCastException());
		verify(template).send(any(ProducerRecord.class));
		recoverer.addNotRetryableExceptions(IllegalStateException.class);
		recoverer.accept(record, new IllegalStateException());
		verify(template, times(2)).send(any(ProducerRecord.class));
		recoverer.removeClassification(IllegalStateException.class);
		recoverer.setFailIfSendResultIsError(false);
		recoverer.accept(record, new IllegalStateException());
		verify(template, times(3)).send(any(ProducerRecord.class));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void headerBitsTurnedOffOneByOne() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(producerRecordCaptor.capture());
		ProducerRecord outRecord = producerRecordCaptor.getValue();
		Headers headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(10);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader(HeaderNames.HeadersToAdd.TOPIC);
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(9);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.PARTITION));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(8);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.OFFSET));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(7);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.TS));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(6);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.TS_TYPE));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(5);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.GROUP));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(4);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.EXCEPTION));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(3);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.EX_CAUSE));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(2);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader((HeaderNames.HeadersToAdd.EX_MSG));
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(1);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();

		recoverer.excludeHeader(HeaderNames.HeadersToAdd.EX_STACKTRACE);
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		outRecord = producerRecordCaptor.getValue();
		headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(0);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNull();

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void headerCreator() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setExceptionHeadersCreator((kafkaHeaders, exception, isKey, headerNames) -> {
			kafkaHeaders.add(new RecordHeader("foo", "bar".getBytes()));
		});
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template, atLeastOnce()).send(producerRecordCaptor.capture());
		ProducerRecord outRecord = producerRecordCaptor.getValue();
		Headers headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(7);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader("foo")).isNotNull();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void addHeaderFunctionsProcessedInOrder() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setHeadersFunction((rec, ex) -> {
			return new RecordHeaders(new RecordHeader[] { new RecordHeader("foo", "one".getBytes()) });
		});
		recoverer.addHeadersFunction((rec, ex) -> {
			return new RecordHeaders(new RecordHeader[] { new RecordHeader("bar", "two".getBytes()) });
		});
		recoverer.addHeadersFunction((rec, ex) -> {
			return new RecordHeaders(new RecordHeader[] { new RecordHeader("foo", "three".getBytes()) });
		});
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(producerRecordCaptor.capture());
		ProducerRecord outRecord = producerRecordCaptor.getValue();
		Headers headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(13);
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
		assertThat(headers.lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();
		assertThat(headers.headers("foo")).extracting("value").containsExactly("one".getBytes(), "three".getBytes());
		assertThat(headers.lastHeader("bar")).extracting("value").isEqualTo("two".getBytes());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void immutableHeaders() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setHeadersFunction((rec, ex) -> {
			RecordHeaders headers = new RecordHeaders(new RecordHeader[] { new RecordHeader("foo", "one".getBytes()) });
			headers.setReadOnly();
			return headers;
		});
		recoverer.addHeadersFunction((rec, ex) -> {
			return new RecordHeaders(new RecordHeader[] { new RecordHeader("bar", "two".getBytes()) });
		});
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(producerRecordCaptor.capture());
		ProducerRecord outRecord = producerRecordCaptor.getValue();
		Headers headers = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(headers, "headers", List.class)).hasSize(12);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void replaceNotAppendHeader() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		Headers headers = new RecordHeaders().add(new RecordHeader("foo", "orig".getBytes()));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
				-1, -1, null, "bar", headers, Optional.empty());
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setHeadersFunction((rec, ex) -> {
			RecordHeaders toReplace = new RecordHeaders(
					new RecordHeader[] { new SingleRecordHeader("foo", "one".getBytes()) });
			return toReplace;
		});
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group", new RuntimeException()));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(producerRecordCaptor.capture());
		ProducerRecord outRecord = producerRecordCaptor.getValue();
		Headers outHeaders = outRecord.headers();
		assertThat(KafkaTestUtils.getPropertyValue(outHeaders, "headers", List.class)).hasSize(11);
		Iterator<Header> iterator = outHeaders.headers("foo").iterator();
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.next().value()).isEqualTo("one".getBytes());
		assertThat(iterator.hasNext()).isFalse();
	}

	@SuppressWarnings("unchecked")
	@Test
	void nonCompliantProducerFactory() throws Exception {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		ProducerFactory pf = mock(ProducerFactory.class);

		willCallRealMethod().given(pf).getConfigurationProperties();

		given(template.getProducerFactory()).willReturn(pf);
		CompletableFuture<?> future = mock(CompletableFuture.class);
		ArgumentCaptor<Long> timeoutCaptor = ArgumentCaptor.forClass(Long.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		given(future.get(timeoutCaptor.capture(), eq(TimeUnit.MILLISECONDS))).willThrow(new TimeoutException());
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setFailIfSendResultIsError(true);
		assertThatThrownBy(() -> recoverer.accept(record, new RuntimeException()))
				.isExactlyInstanceOf(KafkaException.class);
		assertThat(timeoutCaptor.getValue()).isEqualTo(Duration.ofSeconds(125).toMillis());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void blockingRetryRuntimeException() {
		KafkaOperations<?, ?> template = mock(KafkaOperations.class);
		CompletableFuture future = mock(CompletableFuture.class);
		given(template.send(any(ProducerRecord.class))).willReturn(future);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 0, 0L, "bar", null);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.defaultFalse(true);
		recoverer.addRetryableExceptions(RuntimeException.class);
		recoverer.accept(record, new ListenerExecutionFailedException("test", "group",
				new TimestampedException(
						new ListenerExecutionFailedException("test", new ConversionException("test", null)))));
		ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(producerRecordCaptor.capture());
		ProducerRecord outRecord = producerRecordCaptor.getValue();
	}

}
