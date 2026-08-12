/*
 * Copyright 2026-present the original author or authors.
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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify that a batch listener's backoff retry counter for a failing partition is not
 * reset when an unrelated partition's batch succeeds in the same non-concurrent container.
 * See <a href="https://github.com/spring-projects/spring-kafka/issues/4371">GH-4371</a>.
 *
 * @author Soby Chacko
 *
 * @since 4.2
 *
 */
@EmbeddedKafka(
		topics = {
				BatchListenerBackoffMultiPartitionTests.topic,
				BatchListenerBackoffMultiPartitionTests.topicDLT },
		partitions = 2)
public class BatchListenerBackoffMultiPartitionTests {

	public static final String topic = "batchBackoffMultiPart";

	public static final String topicDLT = "batchBackoffMultiPart-dlt";

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	void poisonRecordReachesDltDespiteSuccessfulBatchOnOtherPartition() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "batchBackoffMultiPartGroup", false);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setPollTimeout(3000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);

		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
				(r, e) -> new TopicPartition(topicDLT, 0));

		// 3 retries (4 total attempts) before the record is sent to the DLT
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 3L));

		AtomicInteger partition0FailCount = new AtomicInteger();
		// Latch counting down 4 times (= max attempts) so we can tell the difference
		// between the bug (counter stuck at 1) and a correct exhaustion of retries.
		CountDownLatch partition0Latch = new CountDownLatch(4);

		containerProps.setMessageListener((BatchMessageListener<Integer, String>) records -> {
			for (ConsumerRecord<Integer, String> record : records) {
				if ("poison".equals(record.value())) {
					partition0FailCount.incrementAndGet();
					partition0Latch.countDown();
					throw new BatchListenerFailedException("poison record", record);
				}
			}
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setCommonErrorHandler(errorHandler);
		container.setBeanName("batchBackoffMultiPart");
		container.start();

		// Send poison to partition 0 and a stream of healthy records to partition 1.
		// The healthy records ensure the container processes successful batches from
		// partition 1 between each retry of the partition 0 poison record.
		template.send(topic, 0, null, "poison");
		for (int i = 0; i < 30; i++) {
			template.send(topic, 1, null, "good-" + i);
		}

		// All 4 attempts must happen — if the counter were reset on every partition-1
		// success, the latch would never count down to zero.
		assertThat(partition0Latch.await(60, TimeUnit.SECONDS))
				.as("poison record should be attempted exactly 4 times before DLT recovery")
				.isTrue();

		// Confirm the poison record arrived in the DLT
		Map<String, Object> dltProps = KafkaTestUtils.consumerProps(embeddedKafka, "batchBackoffMultiPartDltGroup", false);
		DefaultKafkaConsumerFactory<Integer, String> dltCf = new DefaultKafkaConsumerFactory<>(dltProps);
		Consumer<Integer, String> dltConsumer = dltCf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(dltConsumer, topicDLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(dltConsumer, topicDLT, Duration.ofSeconds(30));
		assertThat(dltRecord.value()).isEqualTo("poison");

		container.stop();
		pf.destroy();
		dltConsumer.close();
	}

}
