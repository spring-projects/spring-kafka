/*
 * Copyright 2018-present the original author or authors.
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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.util.Assert;

/**
 *
 * Representation of a Destination Topic to which messages can be forwarded, such as retry topics and dlt.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @author Sanghyeok An
 * @since 2.7
 *
 */
public class DestinationTopic {

	private final @Nullable String destinationName;

	private final Properties properties;

	public DestinationTopic(@Nullable String destinationName, Properties properties) {
		this.destinationName = destinationName;
		this.properties = properties;
	}

	public DestinationTopic(String destinationName, DestinationTopic sourceDestinationtopic, String suffix, Type type) {
		this(destinationName, new Properties(sourceDestinationtopic.properties, suffix, type));
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

	public boolean isReusableRetryTopic() {
		return Type.REUSABLE_RETRY_TOPIC.equals(this.properties.type);
	}

	public boolean isMainTopic() {
		return Type.MAIN.equals(this.properties.type);
	}

	public @Nullable String getDestinationName() {
		return this.destinationName;
	}

	public KafkaOperations<?, ?> getKafkaOperations() {
		return this.properties.kafkaOperations;
	}

	public boolean shouldRetryOn(Integer attempt, Throwable e) {
		return this.properties.shouldRetryOn.test(attempt, e);
	}

	public Set<Class<? extends Throwable>> usedForExceptions() {
		return Collections.unmodifiableSet(this.properties.usedForExceptions);
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
		Assert.state(this.destinationName != null, "destination name must not be null");
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

		private final Set<Class<? extends Throwable>> usedForExceptions;

		@Nullable
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
					timeout, null, Collections.emptySet());
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
					sourceProperties.timeout, null, Collections.emptySet());
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
			this(delayMs, suffix, type, maxAttempts, numPartitions, dltStrategy, kafkaOperations, shouldRetryOn,
					timeout, autoStartDltHandler, Collections.emptySet());
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
		 * @param usedForExceptions the exceptions which destination is intended for
		 * @since 3.2
		 */
		public Properties(long delayMs, String suffix, Type type,
				int maxAttempts, int numPartitions,
				DltStrategy dltStrategy,
				KafkaOperations<?, ?> kafkaOperations,
				BiPredicate<Integer, Throwable> shouldRetryOn, long timeout, @Nullable Boolean autoStartDltHandler,
				Set<Class<? extends Throwable>> usedForExceptions) {

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
			this.usedForExceptions = usedForExceptions;
		}

		public boolean isDltTopic() {
			return Type.DLT.equals(this.type);
		}

		public boolean isRetryTopic() {
			return Type.RETRY.equals(this.type) || Type.REUSABLE_RETRY_TOPIC.equals(this.type);
		}

		public String suffix() {
			return this.suffix;
		}

		public long delay() {
			return this.delayMs;
		}

		/**
		 * Return the number of partitions the
		 * retry topics should be created with.
		 * @return the number of partitions.
		 * @since 2.7.13
		 */
		public int numPartitions() {
			return this.numPartitions;
		}

		@Nullable
		public Boolean autoStartDltHandler() {
			return this.autoStartDltHandler;
		}

		public Set<Class<? extends Throwable>> usedForExceptions() {
			return this.usedForExceptions;
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
		MAIN,

		RETRY,

		/**
		 * A retry topic reused along sequential retries
		 * with the same back off interval.
		 *
		 * @since 3.0.4
		 */
		REUSABLE_RETRY_TOPIC,

		DLT,

		NO_OPS
	}
}
