/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Objects;
import java.util.function.BiPredicate;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.lang.Nullable;

/**
 *
 * Representation of a Destination Topic to which messages can be forwarded, such as retry topics and dlt.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class DestinationTopic {

	private final String destinationName;

	private final Properties properties;

	public DestinationTopic(String destinationName, Properties properties) {
		this.destinationName = destinationName;
		this.properties = properties;
	}

	public DestinationTopic(String destinationName, DestinationTopic sourceDestinationtopic, String suffix, Type type) {
		this.destinationName = destinationName;
		this.properties = new Properties(sourceDestinationtopic.properties, suffix, type);
	}

	public Long getDestinationDelay() {
		return this.properties.delayMs;
	}

	public Integer getDestinationPartitions() {
		return this.properties.numPartitions;
	}

	public boolean isAlwaysRetryOnDltFailure() {
		return DltStrategy.ALWAYS_RETRY_ON_ERROR
				.equals(this.properties.dltStrategy);
	}

	public boolean isDltTopic() {
		return Type.DLT.equals(this.properties.type);
	}

	public boolean isNoOpsTopic() {
		return Type.NO_OPS.equals(this.properties.type);
	}

	public boolean isSingleTopicRetry() {
		return Type.SINGLE_TOPIC_RETRY.equals(this.properties.type);
	}

	public boolean isMainTopic() {
		return Type.MAIN.equals(this.properties.type);
	}

	public String getDestinationName() {
		return this.destinationName;
	}

	public KafkaOperations<?, ?> getKafkaOperations() {
		return this.properties.kafkaOperations;
	}

	public boolean shouldRetryOn(Integer attempt, Throwable e) {
		return this.properties.shouldRetryOn.test(attempt, e);
	}

	@Override
	public String toString() {
		return "DestinationTopic{" +
				"destinationName='" + this.destinationName + '\'' +
				", properties=" + this.properties +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DestinationTopic that = (DestinationTopic) o;
		return this.destinationName.equals(that.destinationName) && this.properties.equals(that.properties);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.destinationName, this.properties);
	}

	public long getDestinationTimeout() {
		return this.properties.timeout;
	}

	public static class Properties {

		private final long delayMs;

		private final String suffix;

		private final Type type;

		private final int maxAttempts;

		private final int numPartitions;

		private final DltStrategy dltStrategy;

		private final KafkaOperations<?, ?> kafkaOperations;

		private final BiPredicate<Integer, Throwable> shouldRetryOn;

		private final long timeout;

		private final Boolean autoStartDltHandler;

		/**
		 * Create an instance with the provided properties with the DLT container starting
		 * automatically (if the container factory is so configured).
		 * @param delayMs the delay in ms.
		 * @param suffix the suffix.
		 * @param type the type.
		 * @param maxAttempts the max attempts.
		 * @param numPartitions the number of partitions.
		 * @param dltStrategy the DLT strategy.
		 * @param kafkaOperations the {@link KafkaOperations}.
		 * @param shouldRetryOn the exception classifications.
		 * @param timeout the timeout.
		 */
		public Properties(long delayMs, String suffix, Type type,
						int maxAttempts, int numPartitions,
						DltStrategy dltStrategy,
						KafkaOperations<?, ?> kafkaOperations,
						BiPredicate<Integer, Throwable> shouldRetryOn, long timeout) {

			this(delayMs, suffix, type, maxAttempts, numPartitions, dltStrategy, kafkaOperations, shouldRetryOn,
					timeout, null);
		}

		/**
		 * Create an instance with the provided properties with the DLT container starting
		 * automatically.
		 * @param sourceProperties the source properties.
		 * @param suffix the suffix.
		 * @param type the type.
		 */
		public Properties(Properties sourceProperties, String suffix, Type type) {
			this(sourceProperties.delayMs, suffix, type, sourceProperties.maxAttempts, sourceProperties.numPartitions,
					sourceProperties.dltStrategy, sourceProperties.kafkaOperations, sourceProperties.shouldRetryOn,
					sourceProperties.timeout, null);
		}

		/**
		 * Create an instance with the provided properties.
		 * @param delayMs the delay in ms.
		 * @param suffix the suffix.
		 * @param type the type.
		 * @param maxAttempts the max attempts.
		 * @param numPartitions the number of partitions.
		 * @param dltStrategy the DLT strategy.
		 * @param kafkaOperations the {@link KafkaOperations}.
		 * @param shouldRetryOn the exception classifications.
		 * @param timeout the timeout.
		 * @param autoStartDltHandler whether or not to start the DLT handler.
		 * @since 2.8
		 */
		public Properties(long delayMs, String suffix, Type type,
				int maxAttempts, int numPartitions,
				DltStrategy dltStrategy,
				KafkaOperations<?, ?> kafkaOperations,
				BiPredicate<Integer, Throwable> shouldRetryOn, long timeout, @Nullable Boolean autoStartDltHandler) {

			this.delayMs = delayMs;
			this.suffix = suffix;
			this.type = type;
			this.maxAttempts = maxAttempts;
			this.numPartitions = numPartitions;
			this.dltStrategy = dltStrategy;
			this.kafkaOperations = kafkaOperations;
			this.shouldRetryOn = shouldRetryOn;
			this.timeout = timeout;
			this.autoStartDltHandler = autoStartDltHandler;
		}

		public boolean isDltTopic() {
			return Type.DLT.equals(this.type);
		}

		public String suffix() {
			return this.suffix;
		}

		public long delay() {
			return this.delayMs;
		}

		@Nullable
		public Boolean autoStartDltHandler() {
			return this.autoStartDltHandler;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Properties that = (Properties) o;
			return this.delayMs == that.delayMs
					&& this.maxAttempts == that.maxAttempts
					&& this.numPartitions == that.numPartitions
					&& this.suffix.equals(that.suffix)
					&& this.type == that.type
					&& this.dltStrategy == that.dltStrategy
					&& this.kafkaOperations.equals(that.kafkaOperations);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.delayMs, this.suffix, this.type, this.maxAttempts, this.numPartitions,
					this.dltStrategy, this.kafkaOperations);
		}

		@Override
		public String toString() {
			return "Properties{" +
					"delayMs=" + this.delayMs +
					", suffix='" + this.suffix + '\'' +
					", type=" + this.type +
					", maxAttempts=" + this.maxAttempts +
					", numPartitions=" + this.numPartitions +
					", dltStrategy=" + this.dltStrategy +
					", kafkaOperations=" + this.kafkaOperations +
					", shouldRetryOn=" + this.shouldRetryOn +
					", timeout=" + this.timeout +
					'}';
		}

		public boolean isMainEndpoint() {
			return Type.MAIN.equals(this.type);
		}
	}

	enum Type {
		MAIN, RETRY, SINGLE_TOPIC_RETRY, DLT, NO_OPS
	}
}
