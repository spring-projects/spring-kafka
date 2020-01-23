/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.kafka.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.validation.Valid;
import javax.validation.ValidationException;
import javax.validation.constraints.Max;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.web.JsonPath;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.ProjectingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dariusz Szablinski
 * @author Venil Noronha
 * @author Dimitri Penner
 * @author Nakul Mishra
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { "annotated1", "annotated2", "annotated3",
		"annotated4", "annotated5", "annotated6", "annotated7", "annotated8", "annotated8reply",
		"annotated9", "annotated10",
		"annotated11", "annotated12", "annotated13", "annotated14", "annotated15", "annotated16", "annotated17",
		"annotated18", "annotated19", "annotated20", "annotated21", "annotated21reply", "annotated22",
		"annotated22reply", "annotated23", "annotated23reply", "annotated24", "annotated24reply",
		"annotated25", "annotated25reply1", "annotated25reply2", "annotated26", "annotated27", "annotated28",
		"annotated29", "annotated30", "annotated30reply", "annotated31", "annotated32", "annotated33",
		"annotated34", "annotated35", "annotated36", "annotated37", "foo", "manualStart", "seekOnIdle",
		"annotated38", "annotated38reply", "annotated39"})
public class EnableKafkaIntegrationTests {

	private static final String DEFAULT_TEST_GROUP_ID = "testAnnot";

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private Config config;

	@Autowired
	public Listener listener;

	@Autowired
	public IfaceListenerImpl ifaceListener;

	@Autowired
	public MultiListenerBean multiListener;

	@Autowired
	public MultiJsonListenerBean multiJsonListener;

	@Autowired
	public KafkaTemplate<Integer, String> template;

	@Autowired
	public KafkaTemplate<Integer, String> kafkaJsonTemplate;

	@Autowired
	public KafkaTemplate<byte[], String> bytesKeyTemplate;

	@Autowired
	public KafkaListenerEndpointRegistry registry;

	@Autowired
	private RecordPassAllFilter recordFilter;

	@Autowired
	private DefaultKafkaConsumerFactory<Integer, CharSequence> consumerFactory;

	@Autowired
	private AtomicReference<Consumer<?, ?>> consumerRef;

	@Autowired
	private List<?> quxGroup;

	@Autowired
	private FooConverter fooConverter;

	@Autowired
	private ConcurrentKafkaListenerContainerFactory<Integer, String> transactionalFactory;

	@Autowired
	private SeekToLastOnIdleListener seekOnIdleListener;

	@Autowired
	private MeterRegistry meterRegistry;

	@Test
	public void testAnonymous() {
		MessageListenerContainer container = this.registry
				.getListenerContainer("org.springframework.kafka.KafkaListenerEndpointContainer#0");
		List<?> containers = KafkaTestUtils.getPropertyValue(container, "containers", List.class);
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.consumerGroupId"))
				.isEqualTo(DEFAULT_TEST_GROUP_ID);
		container.stop();
	}

	@Test
	public void testSimple() throws Exception {
		this.recordFilter.called = false;
		template.send("annotated1", 0, "foo");
		template.flush();
		assertThat(this.listener.latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.globalErrorThrowable).isNotNull();
		assertThat(this.listener.receivedGroupId).isEqualTo("foo");

		template.send("annotated2", 0, 123, "foo");
		template.flush();
		assertThat(this.listener.latch2.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.key).isEqualTo(123);
		assertThat(this.listener.partition).isNotNull();
		assertThat(this.listener.topic).isEqualTo("annotated2");
		assertThat(this.listener.receivedGroupId).isEqualTo("bar");

		assertThat(this.meterRegistry.get("spring.kafka.listener")
			.tag("name", "bar-0")
			.tag("extraTag", "foo")
			.tag("result", "success")
			.timer()
			.count())
		.isEqualTo(2L);

		template.send("annotated3", 0, "foo");
		template.flush();
		assertThat(this.listener.latch3.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.capturedRecord.value()).isEqualTo("foo");
		assertThat(this.config.listen3Exception).isNotNull();

		template.send("annotated4", 0, "foo");
		template.flush();
		assertThat(this.listener.latch4.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.capturedRecord.value()).isEqualTo("foo");
		assertThat(this.listener.ack).isNotNull();
		assertThat(this.listener.eventLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.event.getListenerId().startsWith("qux-"));
		MessageListenerContainer manualContainer = this.registry.getListenerContainer("qux");
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener"))
				.isInstanceOf(FilteringMessageListenerAdapter.class);
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener.ackDiscarded",
				Boolean.class)).isTrue();
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener.delegate"))
				.isInstanceOf(RetryingMessageListenerAdapter.class);
		assertThat(KafkaTestUtils
				.getPropertyValue(manualContainer, "containerProperties.messageListener.delegate.recoveryCallback")
				.getClass().getName()).contains("EnableKafkaIntegrationTests$Config$");
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer,
				"containerProperties.messageListener.delegate.delegate"))
				.isInstanceOf(MessagingMessageListenerAdapter.class);
		assertThat(this.listener.listen4Consumer).isNotNull();
		assertThat(this.listener.listen4Consumer).isSameAs(KafkaTestUtils.getPropertyValue(KafkaTestUtils
						.getPropertyValue(this.registry.getListenerContainer("qux"), "containers", List.class).get(0),
				"listenerConsumer.consumer"));
		assertThat(
				KafkaTestUtils.getPropertyValue(this.listener.listen4Consumer, "fetcher.maxPollRecords", Integer.class))
				.isEqualTo(100);
		assertThat(this.quxGroup).hasSize(1);
		assertThat(this.quxGroup.get(0)).isSameAs(manualContainer);
		List<?> containers = KafkaTestUtils.getPropertyValue(manualContainer, "containers", List.class);
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.consumerGroupId"))
				.isEqualTo("qux");
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.consumer.clientId"))
				.isEqualTo("clientIdViaProps3-0");

		template.send("annotated5", 0, 0, "foo");
		template.send("annotated5", 1, 0, "bar");
		template.send("annotated6", 0, 0, "baz");
		template.send("annotated6", 1, 0, "qux");
		template.flush();
		assertThat(this.listener.latch5.await(60, TimeUnit.SECONDS)).isTrue();
		MessageListenerContainer fizConcurrentContainer = registry.getListenerContainer("fiz");
		assertThat(fizConcurrentContainer).isNotNull();
		MessageListenerContainer fizContainer = (MessageListenerContainer) KafkaTestUtils
				.getPropertyValue(fizConcurrentContainer, "containers", List.class).get(0);
		TopicPartitionOffset offset = KafkaTestUtils.getPropertyValue(fizContainer, "topicPartitions",
				TopicPartitionOffset[].class)[2];
		assertThat(offset.isRelativeToCurrent()).isFalse();
		offset = KafkaTestUtils.getPropertyValue(fizContainer, "topicPartitions",
				TopicPartitionOffset[].class)[3];
		assertThat(offset.isRelativeToCurrent()).isTrue();
		assertThat(KafkaTestUtils.getPropertyValue(fizContainer, "listenerConsumer.consumer.groupId"))
				.isEqualTo("fiz");
		assertThat(KafkaTestUtils.getPropertyValue(fizContainer, "listenerConsumer.consumer.clientId"))
				.isEqualTo("clientIdViaAnnotation-0");

		MessageListenerContainer rebalanceConcurrentContainer = registry.getListenerContainer("rebalanceListener");
		assertThat(rebalanceConcurrentContainer).isNotNull();
		assertThat(rebalanceConcurrentContainer.isAutoStartup()).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(rebalanceConcurrentContainer, "concurrency", Integer.class))
				.isEqualTo(3);
		rebalanceConcurrentContainer.start();

		template.send("annotated11", 0, "foo");
		assertThat(this.listener.latch7.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.consumerRef.get()).isNotNull();
		assertThat(this.listener.latch7String).isEqualTo("foo");

		assertThat(this.recordFilter.called).isTrue();

		template.send("annotated11", 0, null);
		assertThat(this.listener.latch7a.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.latch7String).isNull();

		MessageListenerContainer rebalanceContainer = (MessageListenerContainer) KafkaTestUtils
				.getPropertyValue(rebalanceConcurrentContainer, "containers", List.class).get(0);
		assertThat(KafkaTestUtils.getPropertyValue(rebalanceContainer, "listenerConsumer.consumer.groupId"))
				.isNotEqualTo("rebalanceListener");
		String clientId = KafkaTestUtils.getPropertyValue(rebalanceContainer, "listenerConsumer.consumer.clientId",
				String.class);
		assertThat(clientId).startsWith("rebal-");
		assertThat(clientId.indexOf('-')).isEqualTo(clientId.lastIndexOf('-'));
	}

	@Test
	public void testAutoStartup() {
		MessageListenerContainer listenerContainer = registry.getListenerContainer("manualStart");
		assertThat(listenerContainer).isNotNull();
		assertThat(listenerContainer.isRunning()).isFalse();
		assertThat(listenerContainer.getContainerProperties().getSyncCommitTimeout()).isNull();
		this.registry.start();
		assertThat(listenerContainer.isRunning()).isTrue();
		assertThat(((ConcurrentMessageListenerContainer<?, ?>) listenerContainer)
				.getContainers()
				.get(0)
				.getContainerProperties().getSyncCommitTimeout())
				.isEqualTo(Duration.ofSeconds(60));
		assertThat(listenerContainer.getContainerProperties().getSyncCommitTimeout())
				.isEqualTo(Duration.ofSeconds(60));
		listenerContainer.stop();
		assertThat(KafkaTestUtils.getPropertyValue(listenerContainer, "containerProperties.syncCommits", Boolean.class))
				.isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(listenerContainer, "containerProperties.commitCallback"))
				.isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(listenerContainer, "containerProperties.consumerRebalanceListener"))
				.isNotNull();
	}

	@Test
	public void testInterface() throws Exception {
		template.send("annotated7", 0, "foo");
		template.flush();
		assertThat(this.ifaceListener.getLatch1().await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testMulti() throws Exception {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated8reply");
		this.template.send("annotated8", 0, 1, "foo");
		this.template.send("annotated8", 0, 1, null);
		this.template.flush();
		assertThat(this.multiListener.latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiListener.latch2.await(60, TimeUnit.SECONDS)).isTrue();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated8reply");
		assertThat(reply.value()).isEqualTo("OK");
		consumer.close();

		template.send("annotated8", 0, 1, "junk");
		assertThat(this.multiListener.errorLatch.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testMultiJson() throws Exception {
		this.kafkaJsonTemplate.setDefaultTopic("annotated33");
		this.kafkaJsonTemplate.send(new GenericMessage<>(new Foo("one")));
		this.kafkaJsonTemplate.send(new GenericMessage<>(new Baz("two")));
		this.kafkaJsonTemplate.send(new GenericMessage<>(new Qux("three")));
		assertThat(this.multiJsonListener.latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiJsonListener.latch2.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiJsonListener.latch3.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiJsonListener.foo.getBar()).isEqualTo("one");
		assertThat(this.multiJsonListener.baz.getBar()).isEqualTo("two");
		assertThat(this.multiJsonListener.bar.getBar()).isEqualTo("three");
		assertThat(this.multiJsonListener.bar).isInstanceOf(Qux.class);
	}

	@Test
	public void testTx() throws Exception {
		template.send("annotated9", 0, "foo");
		template.flush();
		assertThat(this.ifaceListener.getLatch2().await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testJson() throws Exception {
		Foo foo = new Foo("bar");
		kafkaJsonTemplate.send(MessageBuilder.withPayload(foo)
				.setHeader(KafkaHeaders.TOPIC, "annotated10")
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		assertThat(this.listener.latch6.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foo.getBar()).isEqualTo("bar");
		MessageListenerContainer buzConcurrentContainer = registry.getListenerContainer("buz");
		assertThat(buzConcurrentContainer).isNotNull();
		MessageListenerContainer buzContainer = (MessageListenerContainer) KafkaTestUtils
				.getPropertyValue(buzConcurrentContainer, "containers", List.class).get(0);
		assertThat(KafkaTestUtils.getPropertyValue(buzContainer, "listenerConsumer.consumer.groupId"))
				.isEqualTo("buz.explicitGroupId");
	}

	@Test
	public void testJsonHeaders() throws Exception {
		ConcurrentMessageListenerContainer<?, ?> container =
				(ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer("jsonHeaders");
		Object messageListener = container.getContainerProperties().getMessageListener();
		DefaultJackson2JavaTypeMapper typeMapper = KafkaTestUtils.getPropertyValue(messageListener,
				"messageConverter.typeMapper", DefaultJackson2JavaTypeMapper.class);
		typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
		assertThat(container).isNotNull();
		Foo foo = new Foo("bar");
		this.kafkaJsonTemplate.send(MessageBuilder.withPayload(foo)
				.setHeader(KafkaHeaders.TOPIC, "annotated31")
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		assertThat(this.listener.latch19.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foo.getBar()).isEqualTo("bar");
	}

	@Test
	public void testNulls() throws Exception {
		template.send("annotated12", null, null);
		assertThat(this.listener.latch8.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testEmpty() throws Exception {
		template.send("annotated13", null, "");
		assertThat(this.listener.latch9.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testBatch() throws Exception {
		this.recordFilter.called = false;
		template.send("annotated14", null, "foo");
		template.send("annotated14", null, "bar");
		assertThat(this.listener.latch10.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(String.class);
		assertThat(this.recordFilter.called).isTrue();
		assertThat(this.config.listen10Exception).isNotNull();
		assertThat(this.listener.receivedGroupId).isEqualTo("list1");

		assertThat(this.config.spyLatch.await(30, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testBatchWitHeaders() throws Exception {
		template.send("annotated15", 0, 1, "foo");
		template.send("annotated15", 0, 1, "bar");
		assertThat(this.listener.latch11.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(String.class);
		list = this.listener.keys;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Integer.class);
		list = this.listener.partitions;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Integer.class);
		list = this.listener.topics;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(String.class);
		list = this.listener.offsets;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Long.class);
	}

	@Test
	public void testBatchRecords() throws Exception {
		template.send("annotated16", null, "foo");
		template.send("annotated16", null, "bar");
		assertThat(this.listener.latch12.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(ConsumerRecord.class);
		assertThat(this.listener.listen12Consumer).isNotNull();
		assertThat(this.listener.listen12Consumer).isSameAs(KafkaTestUtils.getPropertyValue(KafkaTestUtils
						.getPropertyValue(this.registry.getListenerContainer("list3"), "containers", List.class).get(0),
				"listenerConsumer.consumer"));
		assertThat(this.config.listen12Latch.await(10, TimeUnit.SECONDS)).isNotNull();
		assertThat(this.config.listen12Exception).isNotNull();
		assertThat(this.config.listen12Message.getPayload()).isInstanceOf(List.class);
		List<?> errorPayload = (List<?>) this.config.listen12Message.getPayload();
		assertThat(errorPayload.size()).isGreaterThanOrEqualTo(1);
	}

	@Test
	public void testBatchRecordsAck() throws Exception {
		template.send("annotated17", null, "foo");
		template.send("annotated17", null, "bar");
		assertThat(this.listener.latch13.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(ConsumerRecord.class);
		assertThat(this.listener.ack).isNotNull();
		assertThat(this.listener.listen13Consumer).isNotNull();
		assertThat(this.listener.listen13Consumer).isSameAs(KafkaTestUtils.getPropertyValue(KafkaTestUtils
						.getPropertyValue(this.registry.getListenerContainer("list4"), "containers", List.class).get(0),
				"listenerConsumer.consumer"));
	}

	@Test
	public void testBatchMessages() throws Exception {
		template.send("annotated18", null, "foo");
		template.send("annotated18", null, "bar");
		assertThat(this.listener.latch14.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Message.class);
		Message<?> m = (Message<?>) list.get(0);
		assertThat(m.getPayload()).isInstanceOf(String.class);
	}

	@Test
	public void testBatchMessagesAck() throws Exception {
		template.send("annotated19", null, "foo");
		template.send("annotated19", null, "bar");
		assertThat(this.listener.latch15.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Message.class);
		Message<?> m = (Message<?>) list.get(0);
		assertThat(m.getPayload()).isInstanceOf(String.class);
		assertThat(this.listener.ack).isNotNull();
	}

	@Test
	public void testListenerErrorHandler() throws Exception {
		template.send("annotated20", 0, "foo");
		template.flush();
		assertThat(this.listener.latch16.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testValidation() throws Exception {
		template.send("annotated35", 0, "{\"bar\":42}");
		assertThat(this.listener.validationLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.validationException).isInstanceOf(ValidationException.class);
	}

	@Test
	public void testReplyingListener() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated21reply");
		template.send("annotated21", 0, "nnotated21reply"); // drop the leading 'a'.
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated21reply");
		assertThat(reply.value()).isEqualTo("NNOTATED21REPLY");
		consumer.close();
	}

	@Test
	public void testReplyingBatchListener() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testBatchReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated22reply");
		template.send("annotated22", 0, 0, "foo");
		template.send("annotated22", 0, 0, "bar");
		template.flush();
		ConsumerRecords<Integer, String> replies = KafkaTestUtils.getRecords(consumer);
		assertThat(replies.count()).isGreaterThanOrEqualTo(1);
		Iterator<ConsumerRecord<Integer, String>> iterator = replies.iterator();
		assertThat(iterator.next().value()).isEqualTo("FOO");
		if (iterator.hasNext()) {
			assertThat(iterator.next().value()).isEqualTo("BAR");
		}
		else {
			replies = KafkaTestUtils.getRecords(consumer);
			assertThat(replies.count()).isGreaterThanOrEqualTo(1);
			iterator = replies.iterator();
			assertThat(iterator.next().value()).isEqualTo("BAR");
		}
		consumer.close();
	}

	@Test
	public void testReplyingListenerWithErrorHandler() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testErrorHandlerReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated23reply");
		template.send("annotated23", 0, "FoO");
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated23reply");
		assertThat(reply.value()).isEqualTo("foo");
		consumer.close();
	}

	@Test
	public void testVoidListenerWithReplyingErrorHandler() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testVoidWithErrorHandlerReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated30reply");
		template.send("annotated30", 0, "FoO");
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated30reply");
		assertThat(reply.value()).isEqualTo("baz");
		consumer.close();
	}

	@Test
	public void testReplyingBatchListenerWithErrorHandler() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testErrorHandlerBatchReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated24reply");
		template.send("annotated24", 0, 0, "FoO");
		template.send("annotated24", 0, 0, "BaR");
		template.flush();
		ConsumerRecords<Integer, String> replies = KafkaTestUtils.getRecords(consumer);
		assertThat(replies.count()).isGreaterThanOrEqualTo(1);
		Iterator<ConsumerRecord<Integer, String>> iterator = replies.iterator();
		assertThat(iterator.next().value()).isEqualTo("foo");
		if (iterator.hasNext()) {
			assertThat(iterator.next().value()).isEqualTo("bar");
		}
		else {
			replies = KafkaTestUtils.getRecords(consumer);
			assertThat(replies.count()).isGreaterThanOrEqualTo(1);
			iterator = replies.iterator();
			assertThat(iterator.next().value()).isEqualTo("bar");
		}
		consumer.close();
	}

	@Test
	public void testMultiReplyTo() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testMultiReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromEmbeddedTopics(consumer, "annotated25reply1", "annotated25reply2");
		template.send("annotated25", 0, 1, "foo");
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated25reply1");
		assertThat(reply.value()).isEqualTo("FOO");
		template.send("annotated25", 0, 1, null);
		reply = KafkaTestUtils.getSingleRecord(consumer, "annotated25reply2");
		assertThat(reply.value()).isEqualTo("BAR");
		consumer.close();
	}

	@Test
	public void testBatchAck() throws Exception {
		template.send("annotated26", 0, 1, "foo1");
		template.send("annotated27", 0, 1, "foo2");
		template.send("annotated27", 0, 1, "foo3");
		template.send("annotated27", 0, 1, "foo4");
		template.flush();
		assertThat(this.listener.latch17.await(60, TimeUnit.SECONDS)).isTrue();
		template.send("annotated26", 0, 1, "foo5");
		template.send("annotated27", 0, 1, "foo6");
		template.flush();
		assertThat(this.listener.latch18.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testBadAckConfig() throws Exception {
		template.send("annotated28", 0, 1, "foo1");
		assertThat(this.config.badAckLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.badAckException).isInstanceOf(IllegalStateException.class);
		assertThat(this.config.badAckException.getMessage())
				.isEqualTo("No Acknowledgment available as an argument, "
						+ "the listener container must have a MANUAL AckMode to populate the Acknowledgment.");
	}

	@Test
	public void testConverterBean() throws Exception {
		@SuppressWarnings("unchecked")
		Converter<String, Foo> converterDelegate = mock(Converter.class);
		fooConverter.setDelegate(converterDelegate);

		Foo foo = new Foo("foo");
		willReturn(foo).given(converterDelegate).convert("{'bar':'foo'}");
		template.send("annotated32", 0, 1, "{'bar':'foo'}");
		assertThat(this.listener.latch20.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.listen16foo).isEqualTo(foo);

		willThrow(new RuntimeException()).given(converterDelegate).convert("foobar");
		template.send("annotated32", 0, 1, "foobar");
		assertThat(this.config.listen16ErrorLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.listen16Exception).isNotNull();
		assertThat(this.config.listen16Exception).isInstanceOf(ListenerExecutionFailedException.class);
		assertThat(((ListenerExecutionFailedException) this.config.listen16Exception).getGroupId())
				.isEqualTo("converter.explicitGroupId");
		assertThat(this.config.listen16Message).isEqualTo("foobar");
	}

	@Test
	public void testAddingTopics() {
		int count = this.embeddedKafka.getTopics().size();
		this.embeddedKafka.addTopics("testAddingTopics");
		assertThat(this.embeddedKafka.getTopics().size()).isEqualTo(count + 1);
		this.embeddedKafka.addTopics(new NewTopic("morePartitions", 10, (short) 1));
		assertThat(this.embeddedKafka.getTopics().size()).isEqualTo(count + 2);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.embeddedKafka.addTopics(new NewTopic("morePartitions", 10, (short) 1)))
				.withMessageContaining("exists");
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.embeddedKafka.addTopics(new NewTopic("morePartitions2", 10, (short) 2)))
				.withMessageContaining("replication");
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testMultiReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		assertThat(consumer.partitionsFor("morePartitions")).hasSize(10);
		consumer.close();
	}

	@Test
	public void testReceivePollResults() throws Exception {
		this.template.send("annotated34", "allRecords");
		assertThat(this.listener.latch21.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.consumerRecords).isNotNull();
		assertThat(this.listener.consumerRecords.count()).isEqualTo(1);
		assertThat(this.listener.consumerRecords.iterator().next().value()).isEqualTo("allRecords");
	}

	@Test
	public void testAutoConfigTm() {
		assertThat(this.transactionalFactory.getContainerProperties().getTransactionManager())
				.isInstanceOf(ChainedKafkaTransactionManager.class);
	}

	@Test
	public void testKeyConversion() throws Exception {
		this.bytesKeyTemplate.send("annotated36", "foo".getBytes(), "bar");
		assertThat(this.listener.keyLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.convertedKey).isEqualTo("foo");
		assertThat(this.config.intercepted).isTrue();
	}

	@Test
	public void testProjection() throws InterruptedException {
		template.send("annotated37", 0, "{ \"username\" : \"SomeUsername\", \"user\" : { \"name\" : \"SomeName\"}}");
		assertThat(this.listener.projectionLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.name).isEqualTo("SomeName");
		assertThat(this.listener.username).isEqualTo("SomeUsername");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSeekToLastOnIdle() throws InterruptedException {
		this.registry.getListenerContainer("seekOnIdle").start();
		this.seekOnIdleListener.waitForBalancedAssignment();
		this.template.send("seekOnIdle", 0, 0, "foo");
		this.template.send("seekOnIdle", 1, 1, "bar");
		assertThat(this.seekOnIdleListener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.seekOnIdleListener.latch2.getCount()).isEqualTo(2L);
		this.seekOnIdleListener.rewindAllOneRecord();
		assertThat(this.seekOnIdleListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.seekOnIdleListener.latch3.getCount()).isEqualTo(1L);
		this.seekOnIdleListener.rewindOnePartitionOneRecord("seekOnIdle", 1);
		assertThat(this.seekOnIdleListener.latch3.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.getListenerContainer("seekOnIdle").stop();
		assertThat(KafkaTestUtils.getPropertyValue(this.seekOnIdleListener, "callbacks", Map.class)).hasSize(0);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReplyingBatchListenerReturnCollection() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testReplyingBatchListenerReturnCollection");
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		ConsumerFactory<Integer, Object> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, Object> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated38reply");
		template.send("annotated38", 0, 0, "FoO");
		template.send("annotated38", 0, 0, "BaR");
		template.flush();
		ConsumerRecords replies = KafkaTestUtils.getRecords(consumer);
		assertThat(replies.count()).isGreaterThanOrEqualTo(1);
		Iterator<ConsumerRecord<?, ?>> iterator = replies.iterator();
		Object value = iterator.next().value();
		assertThat(value).isInstanceOf(List.class);
		List list = (List) value;
		assertThat(list).hasSizeGreaterThanOrEqualTo(1);
		assertThat(list.get(0)).isEqualTo("FOO");
		if (list.size() > 1) {
			assertThat(list.get(1)).isEqualTo("BAR");
		}
		else {
			replies = KafkaTestUtils.getRecords(consumer);
			assertThat(replies.count()).isGreaterThanOrEqualTo(1);
			iterator = replies.iterator();
			value = iterator.next().value();
			list = (List) value;
			assertThat(list).hasSize(1);
			assertThat(list.get(0)).isEqualTo("BAR");
		}
		consumer.close();
	}

	@Test
	public void testCustomMethodArgumentResovlerListener() throws InterruptedException {
		template.send("annotated39", "foo");
		assertThat(this.listener.customMethodArgumentResolverLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.customMethodArgument.body).isEqualTo("foo");
		assertThat(this.listener.customMethodArgument.topic).isEqualTo("annotated39");
	}

	@Configuration
	@EnableKafka
	@EnableTransactionManagement(proxyTargetClass = true)
	public static class Config implements KafkaListenerConfigurer {

		final CountDownLatch spyLatch = new CountDownLatch(2);

		volatile Throwable globalErrorThrowable;

		volatile boolean intercepted;

		@Autowired
		private EmbeddedKafkaBroker embeddedKafka;

		@Bean
		public MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		public static PropertySourcesPlaceholderConfigurer ppc() {
			return new PropertySourcesPlaceholderConfigurer();
		}

		@Bean
		public PlatformTransactionManager transactionManager() {
			return Mockito.mock(PlatformTransactionManager.class);
		}

		@Bean
		public KafkaTransactionManager<Integer, String> ktm() {
			return new KafkaTransactionManager<>(txProducerFactory());
		}

		@Bean
		@Primary
		public ChainedKafkaTransactionManager<Integer, String> cktm() {
			return new ChainedKafkaTransactionManager<>(ktm(), transactionManager());
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setRecordFilterStrategy(recordFilter());
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			factory.setErrorHandler((ConsumerAwareErrorHandler) (t, d, c) -> {
				this.globalErrorThrowable = t;
				c.seek(new org.apache.kafka.common.TopicPartition(d.topic(), d.partition()), d.offset());
			});
			factory.getContainerProperties().setMicrometerTags(Collections.singletonMap("extraTag", "foo"));
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				factoryWithBadConverter() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setRecordFilterStrategy(recordFilter());
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			factory.setErrorHandler((ConsumerAwareErrorHandler) (t, d, c) -> {
				this.globalErrorThrowable = t;
				c.seek(new org.apache.kafka.common.TopicPartition(d.topic(), d.partition()), d.offset());
			});
			factory.getContainerProperties().setMicrometerTags(Collections.singletonMap("extraTag", "foo"));
			factory.setMessageConverter(new RecordMessageConverter() {

				@Override
				public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment,
						Consumer<?, ?> consumer, Type payloadType) {

					throw new UnsupportedOperationException();
				}

				@Override
				public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
					throw new UnsupportedOperationException();				}

			});
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				withNoReplyTemplateContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				transactionalFactory(ObjectProvider<KafkaAwareTransactionManager<Integer, String>> tm) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			KafkaAwareTransactionManager<Integer, String> ktm = tm.getIfUnique();
			if (ktm != null) {
				factory.getContainerProperties().setTransactionManager(ktm);
			}
			return factory;
		}

		@Bean
		public RecordPassAllFilter recordFilter() {
			return new RecordPassAllFilter();
		}

		@Bean
		public RecordPassAllFilter manualFilter() {
			return new RecordPassAllFilter();
		}

		@Bean
		public KafkaListenerContainerFactory<?> kafkaJsonListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			JsonMessageConverter converter = new JsonMessageConverter();
			DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
			typeMapper.addTrustedPackages("*");
			converter.setTypeMapper(typeMapper);
			factory.setMessageConverter(converter);
			return factory;
		}

		/*
		 * Uses Type_Id header
		 */
		@Bean
		public KafkaListenerContainerFactory<?> kafkaJsonListenerContainerFactory2() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			JsonMessageConverter converter = new JsonMessageConverter();
			DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
			typeMapper.addTrustedPackages("*");
			typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
			converter.setTypeMapper(typeMapper);
			factory.setMessageConverter(converter);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> projectionListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			JsonMessageConverter converter = new JsonMessageConverter();
			DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
			typeMapper.addTrustedPackages("*");
			converter.setTypeMapper(typeMapper);
			factory.setMessageConverter(new ProjectingMessageConverter(converter));
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> bytesStringListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<byte[], String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(bytesStringConsumerFactory());
			factory.setRecordInterceptor(record -> {
				this.intercepted = true;
				return record;
			});
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.setRecordFilterStrategy(recordFilter());
			// always send to the same partition so the replies are in order for the test
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchJsonReplyFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.setRecordFilterStrategy(recordFilter());
			// always send to the same partition so the replies are in order for the test
			factory.setReplyTemplate(partitionZeroReplyJsonTemplate());
			return factory;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Bean
		public KafkaListenerContainerFactory<?> batchSpyFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			ConsumerFactory spiedCf = mock(ConsumerFactory.class);
			willAnswer(i -> {
				Consumer<Integer, CharSequence> spy =
						spy(consumerFactory().createConsumer(i.getArgument(0), i.getArgument(1),
								i.getArgument(2), i.getArgument(3)));
				willAnswer(invocation -> {

					try {
						return invocation.callRealMethod();
					}
					finally {
						spyLatch.countDown();
					}

				}).given(spy).commitSync(anyMap(), any());
				return spy;
			}).given(spiedCf).createConsumer(anyString(), anyString(), anyString(), any());
			factory.setConsumerFactory(spiedCf);
			factory.setBatchListener(true);
			factory.setRecordFilterStrategy(recordFilter());
			// always send to the same partition so the replies are in order for the test
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			factory.setMissingTopicsFatal(false);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchManualFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps1"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.MANUAL_IMMEDIATE);
			factory.setBatchListener(true);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchManualFactory2() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps2"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.MANUAL_IMMEDIATE);
			factory.setBatchListener(true);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaManualAckListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps3"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.MANUAL_IMMEDIATE);
			props.setIdleEventInterval(100L);
			props.setPollTimeout(50L);
			factory.setRecordFilterStrategy(manualFilter());
			factory.setAckDiscarded(true);
			factory.setRetryTemplate(new RetryTemplate());
			factory.setRecoveryCallback(c -> null);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaAutoStartFalseListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			ContainerProperties props = factory.getContainerProperties();
			factory.setConsumerFactory(consumerFactory());
			factory.setAutoStartup(false);
			props.setSyncCommits(false);
			props.setCommitCallback(mock(OffsetCommitCallback.class));
			props.setConsumerRebalanceListener(mock(ConsumerRebalanceListener.class));
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaRebalanceListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			ContainerProperties props = factory.getContainerProperties();
			factory.setAutoStartup(true);
			factory.setConsumerFactory(configuredConsumerFactory("rebal"));
			props.setConsumerRebalanceListener(consumerRebalanceListener(consumerRef()));
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				recordAckListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps4"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.RECORD);
			props.setAckOnError(true);
			factory.setErrorHandler(listen16ErrorHandler());
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, CharSequence> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public DefaultKafkaConsumerFactory<byte[], String> bytesStringConsumerFactory() {
			Map<String, Object> configs = consumerConfigs();
			configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			return new DefaultKafkaConsumerFactory<>(configs);
		}

		private ConsumerFactory<Integer, String> configuredConsumerFactory(String clientAndGroupId) {
			Map<String, Object> configs = consumerConfigs();
			configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
			configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientAndGroupId);
			configs.put(ConsumerConfig.GROUP_ID_CONFIG, clientAndGroupId);
			return new DefaultKafkaConsumerFactory<>(configs);
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false", this.embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return consumerProps;
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

		@Bean
		public SeekToLastOnIdleListener seekOnIdle() {
			return new SeekToLastOnIdleListener();
		}

		@Bean
		public IfaceListener<String> ifaceListener() {
			return new IfaceListenerImpl();
		}

		@Bean
		public MultiListenerBean multiListener() {
			return new MultiListenerBean();
		}

		@Bean
		public MultiJsonListenerBean multiJsonListener() {
			return new MultiJsonListenerBean();
		}

		@Bean
		public MultiListenerSendTo multiListenerSendTo() {
			return new MultiListenerSendTo();
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public ProducerFactory<Integer, Object> jsonProducerFactory() {
			Map<String, Object> producerConfigs = producerConfigs();
			producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
			return new DefaultKafkaProducerFactory<>(producerConfigs);
		}

		@Bean
		public ProducerFactory<Integer, String> txProducerFactory() {
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerConfigs());
			pf.setTransactionIdPrefix("tx-");
			return pf;
		}

		@Bean
		public ProducerFactory<byte[], String> bytesStringProducerFactory() {
			Map<String, Object> configs = producerConfigs();
			configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			return new DefaultKafkaProducerFactory<>(configs);
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(this.embeddedKafka);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<>(producerFactory());
		}

		@Bean
		public KafkaTemplate<byte[], String> bytesKeyTemplate() {
			return new KafkaTemplate<>(bytesStringProducerFactory());
		}

		@Bean
		public KafkaTemplate<Integer, String> partitionZeroReplyTemplate() {
			// reply always uses the no-partition, no-key method; subclasses can be used
			return new KafkaTemplate<Integer, String>(producerFactory(), true) {

				@Override
				public ListenableFuture<SendResult<Integer, String>> send(String topic, String data) {
					return super.send(topic, 0, null, data);
				}

			};
		}

		@Bean
		public KafkaTemplate<Integer, Object> partitionZeroReplyJsonTemplate() {
			// reply always uses the no-partition, no-key method; subclasses can be used
			return new KafkaTemplate<Integer, Object>(jsonProducerFactory(), true) {

				@Override
				public ListenableFuture<SendResult<Integer, Object>> send(String topic, Object data) {
					return super.send(topic, 0, null, data);
				}

			};
		}

		@Bean
		public KafkaTemplate<Integer, String> kafkaJsonTemplate() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
			return kafkaTemplate;
		}

		@Bean
		public AtomicReference<Consumer<?, ?>> consumerRef() {
			return new AtomicReference<>();
		}

		private ConsumerAwareRebalanceListener consumerRebalanceListener(
				final AtomicReference<Consumer<?, ?>> consumerRef) {
			return new ConsumerAwareRebalanceListener() {

				@Override
				public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
						Collection<org.apache.kafka.common.TopicPartition> partitions) {
					consumerRef.set(consumer);
				}

				@Override
				public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer,
						Collection<org.apache.kafka.common.TopicPartition> partitions) {
					consumerRef.set(consumer);
				}

				@Override
				public void onPartitionsAssigned(Consumer<?, ?> consumer,
						Collection<org.apache.kafka.common.TopicPartition> partitions) {
					consumerRef.set(consumer);
				}

			};
		}

		@Bean
		public KafkaListenerErrorHandler consumeException(Listener listener) {
			return (m, e) -> {
				listener.latch16.countDown();
				return null;
			};
		}

		@Bean
		public KafkaListenerErrorHandler validationErrorHandler(Listener listener) {
			return (m, e) -> {
				listener.validationException = (Exception) e.getCause();
				listener.validationLatch.countDown();
				return null;
			};
		}

		@Bean
		public KafkaListenerErrorHandler replyErrorHandler() {
			return (m, e) -> ((String) m.getPayload()).toLowerCase();
		}

		@SuppressWarnings("unchecked")
		@Bean
		public KafkaListenerErrorHandler replyBatchErrorHandler() {
			return (m, e) -> ((Collection<String>) m.getPayload())
					.stream()
					.map(String::toLowerCase)
					.collect(Collectors.toList());
		}

		private ListenerExecutionFailedException listen3Exception;

		private ListenerExecutionFailedException listen10Exception;

		private ListenerExecutionFailedException listen12Exception;

		@Bean
		public ConsumerAwareListenerErrorHandler listen3ErrorHandler() {
			return (m, e, c) -> {
				this.listen3Exception = e;
				MessageHeaders headers = m.getHeaders();
				c.seek(new org.apache.kafka.common.TopicPartition(
								headers.get(KafkaHeaders.RECEIVED_TOPIC, String.class),
								headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, Integer.class)),
						headers.get(KafkaHeaders.OFFSET, Long.class));
				return null;
			};
		}

		@Bean
		public ConsumerAwareListenerErrorHandler listen10ErrorHandler() {
			return (m, e, c) -> {
				this.listen10Exception = e;
				resetAllOffsets(m, c);
				return null;
			};
		}

		private Message<?> listen12Message;

		private final CountDownLatch listen12Latch = new CountDownLatch(1);

		@Bean
		public ConsumerAwareListenerErrorHandler listen12ErrorHandler() {
			return (m, e, c) -> {
				this.listen12Exception = e;
				this.listen12Message = m;
				resetAllOffsets(m, c);
				this.listen12Latch.countDown();
				return null;
			};
		}

		protected void resetAllOffsets(Message<?> message, Consumer<?, ?> consumer) {
			MessageHeaders headers = message.getHeaders();
			@SuppressWarnings("unchecked")
			List<String> topics = headers.get(KafkaHeaders.RECEIVED_TOPIC, List.class);
			if (topics == null) {
				return;
			}
			@SuppressWarnings("unchecked")
			List<Integer> partitions = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, List.class);
			@SuppressWarnings("unchecked")
			List<Long> offsets = headers.get(KafkaHeaders.OFFSET, List.class);
			Map<org.apache.kafka.common.TopicPartition, Long> offsetsToReset = new HashMap<>();
			for (int i = 0; i < topics.size(); i++) {
				int index = i;
				offsetsToReset.compute(new org.apache.kafka.common.TopicPartition(topics.get(i), partitions.get(i)),
						(k, v) -> v == null ? offsets.get(index) : Math.min(v, offsets.get(index)));
			}
			offsetsToReset.forEach(consumer::seek);
		}

		private final CountDownLatch badAckLatch = new CountDownLatch(1);

		private Throwable badAckException;

		@Bean
		public KafkaListenerErrorHandler badAckConfigErrorHandler() {
			return (m, e) -> {
				this.badAckException = e.getCause();
				this.badAckLatch.countDown();
				return "baz";
			};
		}

		@Bean
		public KafkaListenerErrorHandler voidSendToErrorHandler() {
			return (m, e) -> {
				return "baz";
			};
		}

		private Throwable listen16Exception;

		private Object listen16Message;

		private final CountDownLatch listen16ErrorLatch = new CountDownLatch(1);

		@Bean
		public ConsumerAwareErrorHandler listen16ErrorHandler() {
			return (e, r, c) -> {
				listen16Exception = e;
				listen16Message = r.value();
				listen16ErrorLatch.countDown();
			};
		}

		@Bean
		public FooConverter fooConverter() {
			return new FooConverter();
		}

		@Override
		public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
			registrar.setValidator(new Validator() {

				@Override
				public void validate(Object target, Errors errors) {
					throw new ValidationException();
				}

				@Override
				public boolean supports(Class<?> clazz) {
					return ValidatedClass.class.isAssignableFrom(clazz);
				}

			});
			registrar.setCustomMethodArgumentResolvers(
					new HandlerMethodArgumentResolver() {

						@Override
						public boolean supportsParameter(MethodParameter parameter) {
							return CustomMethodArgument.class.isAssignableFrom(parameter.getParameterType());
						}

						@Override
						public Object resolveArgument(MethodParameter parameter, Message<?> message) {
							return new CustomMethodArgument(
									(String) message.getPayload(),
									message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC, String.class)
							);
						}

					}
			);

		}

		@Bean
		public KafkaListenerErrorHandler consumeMultiMethodException(MultiListenerBean listener) {
			return (m, e) -> {
				listener.errorLatch.countDown();
				return null;
			};
		}

	}

	@Component
	static class Listener implements ConsumerSeekAware {

		private final ThreadLocal<ConsumerSeekCallback> seekCallBack = new ThreadLocal<>();

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(2); // seek

		final CountDownLatch latch3 = new CountDownLatch(1);

		final CountDownLatch latch4 = new CountDownLatch(1);

		final CountDownLatch latch5 = new CountDownLatch(1);

		final CountDownLatch latch6 = new CountDownLatch(1);

		final CountDownLatch latch7 = new CountDownLatch(1);

		final CountDownLatch latch7a = new CountDownLatch(2);

		volatile String latch7String;

		final CountDownLatch latch8 = new CountDownLatch(1);

		final CountDownLatch latch9 = new CountDownLatch(1);

		final CountDownLatch latch10 = new CountDownLatch(1);

		final CountDownLatch latch11 = new CountDownLatch(1);

		final CountDownLatch latch12 = new CountDownLatch(1);

		final CountDownLatch latch13 = new CountDownLatch(1);

		final CountDownLatch latch14 = new CountDownLatch(1);

		final CountDownLatch latch15 = new CountDownLatch(1);

		final CountDownLatch latch16 = new CountDownLatch(1);

		final CountDownLatch latch17 = new CountDownLatch(4);

		final CountDownLatch latch18 = new CountDownLatch(2);

		final CountDownLatch latch19 = new CountDownLatch(1);

		final CountDownLatch latch20 = new CountDownLatch(1);

		final CountDownLatch latch21 = new CountDownLatch(1);

		final CountDownLatch validationLatch = new CountDownLatch(1);

		final CountDownLatch eventLatch = new CountDownLatch(1);

		final CountDownLatch keyLatch = new CountDownLatch(1);

		final AtomicBoolean reposition12 = new AtomicBoolean();

		final CountDownLatch projectionLatch = new CountDownLatch(1);

		final CountDownLatch customMethodArgumentResolverLatch = new CountDownLatch(1);

		volatile Integer partition;

		volatile ConsumerRecord<?, ?> capturedRecord;

		volatile Acknowledgment ack;

		volatile Object payload;

		volatile Exception validationException;

		volatile String convertedKey;

		volatile Integer key;

		volatile String topic;

		volatile Foo foo;

		volatile Foo listen16foo;

		volatile ListenerContainerIdleEvent event;

		volatile List<Integer> keys;

		volatile List<Integer> partitions;

		volatile List<String> topics;

		volatile List<Long> offsets;

		volatile Consumer<?, ?> listen4Consumer;

		volatile Consumer<?, ?> listen12Consumer;

		volatile Consumer<?, ?> listen13Consumer;

		volatile ConsumerRecords<?, ?> consumerRecords;

		volatile String receivedGroupId;

		volatile String username;

		volatile String name;

		volatile CustomMethodArgument customMethodArgument;

		@KafkaListener(id = "manualStart", topics = "manualStart",
				containerFactory = "kafkaAutoStartFalseListenerContainerFactory")
		public void manualStart(String foo) {
		}

		private final AtomicBoolean reposition1 = new AtomicBoolean();

		@KafkaListener(id = "foo", topics = "#{'${topicOne:annotated1,foo}'.split(',')}")
		public void listen1(String foo, @Header(value = KafkaHeaders.GROUP_ID, required = false) String groupId) {
			this.receivedGroupId = groupId;
			if (this.reposition1.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
			this.latch1.countDown();
		}

		@KafkaListener(id = "bar", topicPattern = "${topicTwo:annotated2}")
		public void listen2(@Payload String foo,
				@Header(KafkaHeaders.GROUP_ID) String groupId,
				@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			this.key = key;
			this.partition = partition;
			this.topic = topic;
			this.receivedGroupId = groupId;
			this.latch2.countDown();
			if (this.latch2.getCount() > 0) {
				this.seekCallBack.get().seek(topic, partition, 0);
			}
		}

		private final AtomicBoolean reposition3 = new AtomicBoolean();

		@KafkaListener(id = "baz", topicPartitions = @TopicPartition(topic = "${topicThree:annotated3}",
				partitions = "${zero:0}"), errorHandler = "listen3ErrorHandler",
				containerFactory = "factoryWithBadConverter")
		public void listen3(ConsumerRecord<?, ?> record) {
			if (this.reposition3.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
			this.capturedRecord = record;
			this.latch3.countDown();
		}

		@KafkaListener(id = "#{'qux'}", topics = "annotated4",
				containerFactory = "kafkaManualAckListenerContainerFactory", containerGroup = "qux#{'Group'}",
				properties = {
						"max.poll.interval.ms:#{'${poll.interval:60000}'}",
						ConsumerConfig.MAX_POLL_RECORDS_CONFIG + "=#{'${poll.recs:100}'}"
				})
		public void listen4(@Payload String foo, Acknowledgment ack, Consumer<?, ?> consumer) {
			this.ack = ack;
			this.ack.acknowledge();
			this.listen4Consumer = consumer;
			this.latch4.countDown();
		}

		@EventListener(condition = "event.listenerId.startsWith('qux')")
		public void eventHandler(ListenerContainerIdleEvent event) {
			this.event = event;
			eventLatch.countDown();
		}

		@KafkaListener(id = "fiz", topicPartitions = {
				@TopicPartition(topic = "annotated5", partitions = { "#{'${foo:0,1}'.split(',')}" }),
				@TopicPartition(topic = "annotated6", partitions = "0",
						partitionOffsets = @PartitionOffset(partition = "${xxx:1}", initialOffset = "${yyy:0}",
								relativeToCurrent = "${zzz:true}"))
		}, clientIdPrefix = "${foo.xxx:clientIdViaAnnotation}")
		public void listen5(ConsumerRecord<?, ?> record) {
			this.capturedRecord = record;
			this.latch5.countDown();
		}

		@KafkaListener(id = "buz", topics = "annotated10", containerFactory = "kafkaJsonListenerContainerFactory",
				groupId = "buz.explicitGroupId")
		public void listen6(Foo foo) {
			this.foo = foo;
			this.latch6.countDown();
		}

		@KafkaListener(id = "jsonHeaders", topics = "annotated31",
				containerFactory = "kafkaJsonListenerContainerFactory",
				groupId = "jsonHeaders")
		public void jsonHeaders(Bar foo) { // should be mapped to Foo via Headers
			this.foo = (Foo) foo;
			this.latch19.countDown();
		}

		@KafkaListener(id = "rebalanceListener", topics = "annotated11", idIsGroup = false,
				containerFactory = "kafkaRebalanceListenerContainerFactory", autoStartup = "${foobarbaz:false}",
				concurrency = "${fixbux:3}")
		public void listen7(@Payload(required = false) String foo) {
			this.latch7String = foo;
			this.latch7.countDown();
			this.latch7a.countDown();
		}

		@KafkaListener(id = "quux", topics = "annotated12")
		public void listen8(@Payload(required = false) String none) {
			assertThat(none).isNull();
			this.latch8.countDown();
		}

		@KafkaListener(id = "corge", topics = "annotated13")
		public void listen9(Object payload) {
			assertThat(payload).isNotNull();
			this.latch9.countDown();
		}

		private final AtomicBoolean reposition10 = new AtomicBoolean();

		@KafkaListener(id = "list1", topics = "annotated14", containerFactory = "batchSpyFactory",
				errorHandler = "listen10ErrorHandler")
		public void listen10(List<String> list, @Header(KafkaHeaders.GROUP_ID) String groupId) {
			if (this.reposition10.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
			this.payload = list;
			this.receivedGroupId = groupId;
			this.latch10.countDown();
		}

		@KafkaListener(id = "list2", topics = "annotated15", containerFactory = "batchFactory")
		public void listen11(List<String> list,
				@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<Integer> keys,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
				@Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
				@Header(KafkaHeaders.OFFSET) List<Long> offsets) {
			this.payload = list;
			this.keys = keys;
			this.partitions = partitions;
			this.topics = topics;
			this.offsets = offsets;
			this.latch11.countDown();
		}

		@KafkaListener(id = "list3", topics = "annotated16", containerFactory = "batchFactory",
				errorHandler = "listen12ErrorHandler")
		public void listen12(List<ConsumerRecord<Integer, String>> list, Consumer<?, ?> consumer) {
			this.payload = list;
			this.listen12Consumer = consumer;
			this.latch12.countDown();
			if (this.reposition12.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
		}

		@KafkaListener(id = "list4", topics = "annotated17", containerFactory = "batchManualFactory")
		public void listen13(List<ConsumerRecord<Integer, String>> list, Acknowledgment ack, Consumer<?, ?> consumer) {
			this.payload = list;
			this.ack = ack;
			ack.acknowledge();
			this.listen13Consumer = consumer;
			this.latch13.countDown();
		}

		@KafkaListener(id = "list5", topics = "annotated18", containerFactory = "batchFactory")
		public void listen14(List<Message<?>> list) {
			this.payload = list;
			this.latch14.countDown();
		}

		@KafkaListener(id = "list6", topics = "annotated19", containerFactory = "batchManualFactory2")
		public void listen15(List<Message<?>> list, Acknowledgment ack) {
			this.payload = list;
			this.ack = ack;
			ack.acknowledge();
			this.latch15.countDown();
		}

		@KafkaListener(id = "converter", topics = "annotated32", containerFactory = "recordAckListenerContainerFactory",
				groupId = "converter.explicitGroupId")
		public void listen16(Foo foo) {
			this.listen16foo = foo;
			this.latch20.countDown();
		}

		@KafkaListener(id = "errorHandler", topics = "annotated20", errorHandler = "consumeException")
		public String errorHandler(String data) throws Exception {
			throw new Exception("return this");
		}

		@KafkaListener(id = "replyingListener", topics = "annotated21")
		@SendTo("a!{request.value()}") // runtime SpEL - test payload is the reply queue minus leading 'a'
		public String replyingListener(String in) {
			return in.toUpperCase();
		}

		@KafkaListener(id = "replyingBatchListener", topics = "annotated22", containerFactory = "batchFactory")
		@SendTo("a#{'nnotated22reply'}") // config time SpEL
		public Collection<String> replyingBatchListener(List<String> in) {
			return in.stream().map(String::toUpperCase).collect(Collectors.toList());
		}

		@KafkaListener(id = "replyingListenerWithErrorHandler", topics = "annotated23",
				errorHandler = "replyErrorHandler")
		@SendTo("${foo:annotated23reply}")
		public String replyingListenerWithErrorHandler(String in) {
			throw new RuntimeException("return this");
		}

		@KafkaListener(id = "voidListenerWithReplyingErrorHandler", topics = "annotated30",
				errorHandler = "voidSendToErrorHandler")
		@SendTo("annotated30reply")
		public void voidListenerWithReplyingErrorHandler(String in) {
			throw new RuntimeException("fail");
		}

		@KafkaListener(id = "replyingBatchListenerWithErrorHandler", topics = "annotated24",
				containerFactory = "batchFactory", errorHandler = "replyBatchErrorHandler")
		@SendTo("annotated24reply")
		public Collection<String> replyingBatchListenerWithErrorHandler(List<String> in) {
			throw new RuntimeException("return this");
		}

		@KafkaListener(id = "replyingBatchListenerCollection", topics = "annotated38",
				containerFactory = "batchJsonReplyFactory", splitIterables = false)
		@SendTo("annotated38reply")
		public Collection<String> replyingBatchListenerCollection(List<String> in) {
			return in.stream()
					.map(String::toUpperCase)
					.collect(Collectors.toList());
		}

		@KafkaListener(id = "batchAckListener", topics = { "annotated26", "annotated27" },
				containerFactory = "batchFactory")
		public void batchAckListener(@SuppressWarnings("unused") List<String> in,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitionsHeader,
				@Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topicsHeader,
				Consumer<?, ?> consumer) {

			for (int i = 0; i < topicsHeader.size(); i++) {
				this.latch17.countDown();
				String inTopic = topicsHeader.get(i);
				if ("annotated26".equals(inTopic) && consumer.committed(Collections.singleton(
						new org.apache.kafka.common.TopicPartition(inTopic, partitionsHeader.get(i))))
							.values()
							.iterator()
							.next()
							.offset() == 1) {
					this.latch18.countDown();
				}
				else if ("annotated27".equals(inTopic) && consumer.committed(Collections.singleton(
						new org.apache.kafka.common.TopicPartition(inTopic, partitionsHeader.get(i))))
							.values()
							.iterator()
							.next()
							.offset() == 3) {
					this.latch18.countDown();
				}
			}
		}

		@KafkaListener(id = "ackWithAutoContainer", topics = "annotated28", errorHandler = "badAckConfigErrorHandler",
				containerFactory = "withNoReplyTemplateContainerFactory")
		public void ackWithAutoContainerListener(String payload, Acknowledgment ack) {
			// empty
		}

		@KafkaListener(id = "bytesKey", topics = "annotated36",
				containerFactory = "bytesStringListenerContainerFactory")
		public void bytesKey(String in, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
			this.convertedKey = key;
			this.keyLatch.countDown();
		}

		@KafkaListener(topics = "annotated29")
		public void anonymousListener(String in) {
		}

		@KafkaListener(id = "pollResults", topics = "annotated34", containerFactory = "batchFactory")
		public void pollResults(ConsumerRecords<?, ?> records) {
			this.consumerRecords = records;
			this.latch21.countDown();
		}

		@KafkaListener(id = "validated", topics = "annotated35", errorHandler = "validationErrorHandler",
				containerFactory = "kafkaJsonListenerContainerFactory")
		public void validatedListener(@Payload @Valid ValidatedClass val) {
			// NOSONAR
		}

		@KafkaListener(id = "projection", topics = "annotated37",
				containerFactory = "projectionListenerContainerFactory")
		public void projectionListener(ProjectionSample sample) {
			this.username = sample.getUsername();
			this.name = sample.getName();
			this.projectionLatch.countDown();
		}

		@KafkaListener(id = "customMethodArgumentResolver", topics = "annotated39")
		public void customMethodArgumentResolverListener(String data, CustomMethodArgument customMethodArgument) {
			this.customMethodArgument = customMethodArgument;
			this.customMethodArgumentResolverLatch.countDown();
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			this.seekCallBack.set(callback);
		}

	}

	public static class SeekToLastOnIdleListener extends AbstractConsumerSeekAware {

		private final CountDownLatch latch1 = new CountDownLatch(10);

		private final CountDownLatch latch2 = new CountDownLatch(12);

		private final CountDownLatch latch3 = new CountDownLatch(13);

		private final Set<Thread> consumerThreads = ConcurrentHashMap.newKeySet();

		@KafkaListener(id = "seekOnIdle", topics = "seekOnIdle", autoStartup = "false", concurrency = "2",
				clientIdPrefix = "seekOnIdle", containerFactory = "kafkaManualAckListenerContainerFactory")
		public void listen(@SuppressWarnings("unused") String in, Acknowledgment ack) {
			this.latch3.countDown();
			this.latch2.countDown();
			this.latch1.countDown();
			ack.acknowledge();
		}

		@Override
		public void onIdleContainer(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
				ConsumerSeekCallback callback) {

			if (this.latch1.getCount() > 0) {
				assignments.keySet().forEach(tp -> callback.seekRelative(tp.topic(), tp.partition(), -1, true));
			}
		}

		public void rewindAllOneRecord() {
			getSeekCallbacks()
				.forEach((tp, callback) ->
					callback.seekRelative(tp.topic(), tp.partition(), -1, true));
		}

		public void rewindOnePartitionOneRecord(String topic, int partition) {
			getSeekCallbackFor(new org.apache.kafka.common.TopicPartition(topic, partition))
				.seekRelative(topic, partition, -1, true);
		}

		@Override
		public synchronized void onPartitionsAssigned(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
				ConsumerSeekCallback callback) {

			super.onPartitionsAssigned(assignments, callback);
			if (assignments.size() > 0) {
				this.consumerThreads.add(Thread.currentThread());
				notifyAll();
			}
		}

		public synchronized void waitForBalancedAssignment() throws InterruptedException {
			int n = 0;
			while (this.consumerThreads.size() < 2) {
				wait(1000);
				if (n++ > 20) {
					throw new IllegalStateException("Balanced distribution did not occur");
				}
			}
		}

	}

	interface IfaceListener<T> {

		void listen(T foo);

	}

	static class IfaceListenerImpl implements IfaceListener<String> {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		@Override
		@KafkaListener(id = "ifc", topics = "annotated7")
		public void listen(String foo) {
			latch1.countDown();
		}

		@KafkaListener(id = "ifctx", topics = "annotated9")
		@Transactional(transactionManager = "transactionManager")
		public void listenTx(String foo) {
			latch2.countDown();
		}

		public CountDownLatch getLatch1() {
			return latch1;
		}

		public CountDownLatch getLatch2() {
			return latch2;
		}

	}

	@KafkaListener(id = "multi", topics = "annotated8", errorHandler = "consumeMultiMethodException")
	static class MultiListenerBean {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		private final CountDownLatch errorLatch = new CountDownLatch(1);

		@KafkaHandler
		public void bar(@NonNull String bar) {
			if ("junk".equals(bar)) {
				throw new RuntimeException("intentional");
			}
			else {
				this.latch1.countDown();
			}
		}

		@KafkaHandler
		@SendTo("#{'${foo:annotated8reply}'}")
		public String bar(@Payload(required = false) KafkaNull nul, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) int key) {
			this.latch2.countDown();
			return "OK";
		}

		public void foo(String bar) {
		}

	}

	@KafkaListener(id = "multiJson", topics = "annotated33", containerFactory = "kafkaJsonListenerContainerFactory2")
	static class MultiJsonListenerBean {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		private final CountDownLatch latch3 = new CountDownLatch(1);

		private Foo foo;

		private Baz baz;

		private Bar bar;

		@KafkaHandler
		public void bar(Foo foo) {
			this.foo = foo;
			this.latch1.countDown();
		}

		@KafkaHandler
		public void bar(Baz baz) {
			this.baz = baz;
			this.latch2.countDown();
		}

		@KafkaHandler(isDefault = true)
		public void defaultHandler(Bar bar) {
			this.bar = bar;
			this.latch3.countDown();
		}

	}

	@KafkaListener(id = "multiSendTo", topics = "annotated25")
	@SendTo("annotated25reply1")
	static class MultiListenerSendTo {

		@KafkaHandler
		public String foo(String in) {
			return in.toUpperCase();
		}

		@KafkaHandler
		@SendTo("!{'annotated25reply2'}")
		public String bar(@Payload(required = false) KafkaNull nul,
				@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) int key) {
			return "BAR";
		}

	}

	public interface Bar {

		String getBar();

	}

	public static class Foo implements Bar {

		private String bar;


		public Foo() {
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		@Override
		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class Baz implements Bar {

		private String bar;

		public Baz() {
		}

		public Baz(String bar) {
			this.bar = bar;
		}

		@Override
		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class Qux implements Bar {

		private String bar;

		public Qux() {
		}

		public Qux(String bar) {
			this.bar = bar;
		}

		@Override
		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class RecordPassAllFilter implements RecordFilterStrategy<Integer, CharSequence> {

		private boolean called;

		@Override
		public boolean filter(ConsumerRecord<Integer, CharSequence> consumerRecord) {
			called = true;
			return false;
		}

	}

	public static class FooConverter implements Converter<String, Foo> {

		private Converter<String, Foo> delegate;

		public Converter<String, Foo> getDelegate() {
			return delegate;
		}

		public void setDelegate(
				Converter<String, Foo> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Foo convert(String source) {
			return delegate.convert(source);
		}

	}

	public static class ValidatedClass {

		@Max(10)
		private int bar;

		public int getBar() {
			return this.bar;
		}

		public void setBar(int bar) {
			this.bar = bar;
		}

	}

	interface ProjectionSample {

		String getUsername();

		@JsonPath("$.user.name")
		String getName();

	}

	static class CustomMethodArgument {

		final String body;

		final String topic;

		CustomMethodArgument(String body, String topic) {
			this.body = body;
			this.topic = topic;
		}

	}


}
