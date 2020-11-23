/*
 * Copyright 2017-2020 the original author or authors.
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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.0.1
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class ManualAssignmentInitialSeekTests {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	/*
	 * Deliver 6 records from three partitions, fail on the second record second
	 * partition, first attempt; verify partition 0,1 committed and a total of 7 records
	 * handled after seek.
	 */
	@SuppressWarnings("unchecked")
	@Test
	void discardRemainingRecordsFromPollAndSeek() throws Exception {
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer);
		inOrder.verify(this.consumer).assign(any(Collection.class));
		inOrder.verify(this.consumer).seekToBeginning(any());
		inOrder.verify(this.consumer, atLeastOnce()).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		assertThat(this.config.registerSeekCallbackCalled).isTrue();
		assertThat(this.config.partitionsAssignedCalled).isTrue();
		assertThat(this.config.assignments).hasSize(3);
	}

	@Test
	void parsePartitions() {
		TopicPartitionOffset[] topicPartitions = registry.getListenerContainer("pp")
				.getContainerProperties()
				.getTopicPartitions();
		List<Integer> collected = Arrays.stream(topicPartitions).map(tp -> tp.getPartition())
				.collect(Collectors.toList());
		assertThat(collected).containsExactly(0, 1, 2, 3, 4, 5, 7, 10, 11, 12, 13, 14, 15);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void parseUnitTests() throws Exception {
		Method parser = KafkaListenerAnnotationBeanPostProcessor.class.getDeclaredMethod("parsePartitions",
				String.class);
		parser.setAccessible(true);
		KafkaListenerAnnotationBeanPostProcessor bpp = new KafkaListenerAnnotationBeanPostProcessor();
		assertThat((Stream<Integer>) parser.invoke(bpp, "0-2")).containsExactly(0, 1, 2);
		assertThat((Stream<Integer>) parser.invoke(bpp, "  0-2  ,  5")).containsExactly(0, 1, 2, 5);
		assertThat((Stream<Integer>) parser.invoke(bpp, "0-2,5-6")).containsExactly(0, 1, 2, 5, 6);
		assertThat((Stream<Integer>) parser.invoke(bpp, "5-6,0-2,0-2")).containsExactly(0, 1, 2, 5, 6);
	}

	@Configuration
	@EnableKafka
	public static class Config extends AbstractConsumerSeekAware {

		final CountDownLatch pollLatch = new CountDownLatch(1);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		volatile boolean registerSeekCallbackCalled;

		volatile boolean partitionsAssignedCalled;

		volatile Map<TopicPartition, Long> assignments;

		@KafkaListener(groupId = "grp",
				topicPartitions = @org.springframework.kafka.annotation.TopicPartition(topic = "foo",
						partitions = "#{'0,1,2'.split(',')}",
						partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")))
		public void foo(String in) {
		}

		@KafkaListener(id = "pp", autoStartup = "false",
				topicPartitions = @org.springframework.kafka.annotation.TopicPartition(topic = "foo",
						partitions = "0-5, 7, 10-15"))
		public void bar(String in) {
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer();
			given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public Consumer consumer() {
			final Consumer consumer = mock(Consumer.class);
			willAnswer(i -> {
				this.pollLatch.countDown();
				try {
					Thread.sleep(50);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				return new ConsumerRecords(Collections.emptyMap());
			}).given(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
			willAnswer(i -> {
				this.closeLatch.countDown();
				return null;
			}).given(consumer).close();
			return consumer;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.setErrorHandler(new SeekToCurrentErrorHandler());
			factory.getContainerProperties().setAckMode(AckMode.RECORD);
			factory.getContainerProperties().setDeliveryAttemptHeader(true);
			return factory;
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			super.registerSeekCallback(callback);
			this.registerSeekCallbackCalled = true;
		}

		@Override
		public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			super.onPartitionsAssigned(assignments, callback);
			this.partitionsAssignedCalled = true;
			this.assignments = assignments;
			callback.seekToBeginning(assignments.keySet());
		}

	}

}
