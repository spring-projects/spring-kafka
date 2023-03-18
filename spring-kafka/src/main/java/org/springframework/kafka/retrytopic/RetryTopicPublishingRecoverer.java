/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * A {@link DeadLetterPublishingRecoverer} that publishes a failed record to a dead-letter
 * topic. In comparison to {@link DeadLetterPublishingRecoverer} it configures event headers
 * specifically for the purpose of publishing to the retry topics.
 *
 * @author Adrian Chlebosz
 * @since 2.8.12
 */
public class RetryTopicPublishingRecoverer extends DeadLetterPublishingRecoverer {

	public RetryTopicPublishingRecoverer(Function<ProducerRecord<?, ?>, KafkaOperations<?, ?>> templateResolver, boolean transactional, BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
		super(templateResolver, transactional, destinationResolver);
	}

	@Override
	protected DeadLetterPublishingRecoverer.HeaderNames getHeaderNames() {
		return DeadLetterPublishingRecoverer.HeaderNames.Builder
			.original()
			.offsetHeader(KafkaHeaders.ORIGINAL_OFFSET)
			.timestampHeader(KafkaHeaders.ORIGINAL_TIMESTAMP)
			.timestampTypeHeader(KafkaHeaders.ORIGINAL_TIMESTAMP_TYPE)
			.topicHeader(KafkaHeaders.ORIGINAL_TOPIC)
			.partitionHeader(KafkaHeaders.ORIGINAL_PARTITION)
			.consumerGroupHeader(KafkaHeaders.ORIGINAL_CONSUMER_GROUP)
			.exception()
			.keyExceptionFqcn(KafkaHeaders.KEY_EXCEPTION_FQCN)
			.exceptionFqcn(KafkaHeaders.EXCEPTION_FQCN)
			.exceptionCauseFqcn(KafkaHeaders.EXCEPTION_CAUSE_FQCN)
			.keyExceptionMessage(KafkaHeaders.KEY_EXCEPTION_MESSAGE)
			.exceptionMessage(KafkaHeaders.EXCEPTION_MESSAGE)
			.keyExceptionStacktrace(KafkaHeaders.KEY_EXCEPTION_STACKTRACE)
			.exceptionStacktrace(KafkaHeaders.EXCEPTION_STACKTRACE)
			.build();
	}

}
