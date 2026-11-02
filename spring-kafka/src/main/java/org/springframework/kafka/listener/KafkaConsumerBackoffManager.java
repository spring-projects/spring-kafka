/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * Interface for backing off a {@link MessageListenerContainer}
 * until a given dueTimestamp, if such timestamp is in the future.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
public interface KafkaConsumerBackoffManager {

	void backOffIfNecessary(Context context);

	default Context createContext(long dueTimestamp, String listenerId, TopicPartition topicPartition,
								@Nullable Consumer<?, ?> messageConsumer) {
		return new Context(dueTimestamp, topicPartition, listenerId, messageConsumer);
	}

	/**
	 * Provides the state that will be used for backing off.
	 * @since 2.7
	 */
	class Context {

		/**
		 * The time after which the message should be processed,
		 * in milliseconds since epoch.
		 */
		private final long dueTimestamp;

		/**
		 * The id for the listener that should be paused.
		 */
		private final String listenerId;

		/**
		 * The topic that contains the partition to be paused.
		 */
		private final TopicPartition topicPartition;

		/**
		 * The consumer of the message, if present.
		 */
		private final @Nullable Consumer<?, ?> consumerForTimingAdjustment;

		Context(long dueTimestamp, TopicPartition topicPartition, String listenerId,
				@Nullable Consumer<?, ?> consumerForTimingAdjustment) {

			this.dueTimestamp = dueTimestamp;
			this.listenerId = listenerId;
			this.topicPartition = topicPartition;
			this.consumerForTimingAdjustment = consumerForTimingAdjustment;
		}

		public long getDueTimestamp() {
			return this.dueTimestamp;
		}

		public String getListenerId() {
			return this.listenerId;
		}

		public TopicPartition getTopicPartition() {
			return this.topicPartition;
		}

		public @Nullable Consumer<?, ?> getConsumerForTimingAdjustment() {
			return this.consumerForTimingAdjustment;
		}

	}

}
