/*
 * Copyright 2016-2017 the original author or authors.
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
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.FilteringAcknowledgingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.listener.adapter.RetryingAcknowledgingMessageListenerAdapter;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dariusz Szablinski
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class EnableKafkaIntegrationTests {

	@Autowired
	public Listener listener;

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true,
			"annotated1", "annotated2", "annotated3",
			"annotated4", "annotated5", "annotated6", "annotated7", "annotated8", "annotated9", "annotated10",
			"annotated11", "annotated12", "annotated13");

	@Autowired
	public IfaceListenerImpl ifaceListener;

	@Autowired
	public MultiListenerBean multiListener;

	@Autowired
	public KafkaTemplate<Integer, String> template;

	@Autowired
	public KafkaTemplate<Integer, String> kafkaJsonTemplate;

	@Autowired
	public KafkaListenerEndpointRegistry registry;

	@Autowired
	private RecordFilterImpl recordFilter;

	@Test
	public void testSimple() throws Exception {
		template.send("annotated1", 0, "foo");
		template.flush();
		assertThat(this.listener.latch1.await(60, TimeUnit.SECONDS)).isTrue();

		template.send("annotated2", 0, 123, "foo");
		template.flush();
		assertThat(this.listener.latch2.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.key).isEqualTo(123);
		assertThat(this.listener.partition).isNotNull();
		assertThat(this.listener.topic).isEqualTo("annotated2");

		template.send("annotated3", 0, "foo");
		template.flush();
		assertThat(this.listener.latch3.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.record.value()).isEqualTo("foo");

		template.send("annotated4", 0, "foo");
		template.flush();
		assertThat(this.listener.latch4.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.record.value()).isEqualTo("foo");
		assertThat(this.listener.ack).isNotNull();
		assertThat(this.listener.eventLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.event.getListenerId().startsWith("qux-"));
		MessageListenerContainer manualContainer = this.registry.getListenerContainer("qux");
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener"))
				.isInstanceOf(FilteringAcknowledgingMessageListenerAdapter.class);
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener.ackDiscarded",
				Boolean.class)).isTrue();
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener.delegate"))
				.isInstanceOf(RetryingAcknowledgingMessageListenerAdapter.class);
		assertThat(KafkaTestUtils
				.getPropertyValue(manualContainer, "containerProperties.messageListener.delegate.recoveryCallback")
				.getClass().getName()).contains("EnableKafkaIntegrationTests$Config$");
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer,
				"containerProperties.messageListener.delegate.delegate"))
						.isInstanceOf(MessagingMessageListenerAdapter.class);

		template.send("annotated5", 0, 0, "foo");
		template.send("annotated5", 1, 0, "bar");
		template.send("annotated6", 0, 0, "baz");
		template.send("annotated6", 1, 0, "qux");
		template.flush();
		assertThat(this.listener.latch5.await(60, TimeUnit.SECONDS)).isTrue();

		template.send("annotated11", 0, "foo");
		template.flush();
		assertThat(this.listener.latch7.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(this.recordFilter.called).isTrue();
	}

	@Test
	public void testAutoStartup() throws Exception {
		MessageListenerContainer listenerContainer = registry.getListenerContainer("manualStart");
		assertThat(listenerContainer).isNotNull();
		assertThat(listenerContainer.isRunning()).isFalse();
		this.registry.start();
		assertThat(listenerContainer.isRunning()).isTrue();
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
		template.send("annotated8", 0, 1, "foo");
		template.send("annotated8", 0, 1, null);
		template.flush();
		assertThat(this.multiListener.latch1.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testTx() throws Exception {
		template.send("annotated9", 0, "foo");
		template.flush();
		assertThat(this.ifaceListener.getLatch2().await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testJson() throws Exception {
		Foo foo = new Foo();
		foo.setBar("bar");
		kafkaJsonTemplate.send(MessageBuilder.withPayload(foo)
				.setHeader(KafkaHeaders.TOPIC, "annotated10")
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		assertThat(this.listener.latch6.await(60, TimeUnit.SECONDS)).isTrue();
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

	@Configuration
	@EnableKafka
	@EnableTransactionManagement(proxyTargetClass = true)
	public static class Config {

		@Bean
		public static PropertySourcesPlaceholderConfigurer ppc() {
			return new PropertySourcesPlaceholderConfigurer();
		}

		@Bean
		public PlatformTransactionManager transactionManager() {
			return Mockito.mock(PlatformTransactionManager.class);
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setRecordFilterStrategy(recordFilter());
			return factory;
		}

		@Bean
		public RecordFilterImpl recordFilter() {
			return new RecordFilterImpl();
		}

		@Bean
		public RecordFilterImpl manualFilter() {
			return new RecordFilterImpl();
		}

		@Bean
		public KafkaListenerContainerFactory<?> kafkaJsonListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setMessageConverter(new StringJsonMessageConverter());
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaManualAckListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(manualConsumerFactory());
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.MANUAL_IMMEDIATE);
			props.setIdleEventInterval(100L);
			factory.setRecordFilterStrategy(manualFilter());
			factory.setAckDiscarded(true);
			factory.setRetryTemplate(new RetryTemplate());
			factory.setRecoveryCallback(new RecoveryCallback<Void>() {

				@Override
				public Void recover(RetryContext context) throws Exception {
					return null;
				}

			});
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
			factory.setConsumerFactory(consumerFactory());
			props.setConsumerRebalanceListener(consumerRebalanceListener());
			return factory;
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public ConsumerFactory<Integer, String> manualConsumerFactory() {
			Map<String, Object> configs = consumerConfigs();
			configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
			return new DefaultKafkaConsumerFactory<>(configs);
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testAnnot", "true", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return consumerProps;
		}

		@Bean
		public Listener listener() {
			return new Listener();
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
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(embeddedKafka);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<Integer, String>(producerFactory());
		}

		@Bean
		public KafkaTemplate<Integer, String> kafkaJsonTemplate() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<Integer, String>(producerFactory());
			kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
			return kafkaTemplate;
		}

		private ConsumerRebalanceListener consumerRebalanceListener() {
			return new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsRevoked(Collection<org.apache.kafka.common.TopicPartition> partitions) {

				}

				@Override
				public void onPartitionsAssigned(Collection<org.apache.kafka.common.TopicPartition> partitions) {

				}

			};
		}

	}

	static class Listener {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		private final CountDownLatch latch3 = new CountDownLatch(1);

		private final CountDownLatch latch4 = new CountDownLatch(1);

		private final CountDownLatch latch5 = new CountDownLatch(1);

		private final CountDownLatch latch6 = new CountDownLatch(1);

		private final CountDownLatch latch7 = new CountDownLatch(1);

		private final CountDownLatch latch8 = new CountDownLatch(1);

		private final CountDownLatch latch9 = new CountDownLatch(1);

		private final CountDownLatch eventLatch = new CountDownLatch(1);

		private volatile Integer partition;

		private volatile ConsumerRecord<?, ?> record;

		private volatile Acknowledgment ack;

		private Integer key;

		private String topic;

		private Foo foo;

		private volatile ListenerContainerIdleEvent event;

		@KafkaListener(id = "manualStart", topics = "manualStart",
				containerFactory = "kafkaAutoStartFalseListenerContainerFactory")
		public void manualStart(String foo) {
		}

		@KafkaListener(id = "foo", topics = "#{'${topicOne:annotated1,foo}'.split(',')}")
		public void listen1(String foo) {
			this.latch1.countDown();
		}

		@KafkaListener(id = "bar", topicPattern = "${topicTwo:annotated2}")
		public void listen2(@Payload String foo,
				@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			this.key = key;
			this.partition = partition;
			this.topic = topic;
			this.latch2.countDown();
		}

		@KafkaListener(id = "baz", topicPartitions = @TopicPartition(topic = "${topicThree:annotated3}",
				partitions = "${zero:0}"))
		public void listen3(ConsumerRecord<?, ?> record) {
			this.record = record;
			this.latch3.countDown();
		}

		@KafkaListener(id = "qux", topics = "annotated4", containerFactory = "kafkaManualAckListenerContainerFactory")
		public void listen4(@Payload String foo, Acknowledgment ack) {
			this.ack = ack;
			this.ack.acknowledge();
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
						partitionOffsets = @PartitionOffset(partition = "${xxx:1}", initialOffset = "${yyy:0}"))
		})
		public void listen5(ConsumerRecord<?, ?> record) {
			this.record = record;
			this.latch5.countDown();
		}

		@KafkaListener(id = "buz", topics = "annotated10", containerFactory = "kafkaJsonListenerContainerFactory")
		public void listen6(Foo foo) {
			this.foo = foo;
			this.latch6.countDown();
		}

		@KafkaListener(id = "rebalancerListener", topics = "annotated11",
				containerFactory = "kafkaRebalanceListenerContainerFactory")
		public void listen7(String foo) {
			this.latch7.countDown();
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
		@Transactional
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

	@KafkaListener(id = "multi", topics = "annotated8")
	static class MultiListenerBean {

		private final CountDownLatch latch1 = new CountDownLatch(2);

		@KafkaHandler
		public void bar(String bar) {
			latch1.countDown();
		}

		@KafkaHandler
		public void bar(@Payload(required = false) KafkaNull nul, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) int key) {
			latch1.countDown();
		}

		public void foo(String bar) {
		}

	}

	public static class Foo {

		private String bar;

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class RecordFilterImpl implements RecordFilterStrategy<Integer, String> {

		private boolean called;

		@Override
		public boolean filter(ConsumerRecord<Integer, String> consumerRecord) {
			called = true;
			return false;
		}

	}

}
