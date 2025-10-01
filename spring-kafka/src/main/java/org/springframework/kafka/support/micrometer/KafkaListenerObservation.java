/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import org.springframework.util.StringUtils;

/**
 * Spring for Apache Kafka Observation for listeners.
 *
 * @author Gary Russell
 * @author Christian Mergenthaler
 * @author Wang Zhiyang
 * @author Christian Fredriksson
 *
 * @since 3.0
 *
 */
public enum KafkaListenerObservation implements ObservationDocumentation {

	/**
	 * Observation for Apache Kafka listeners.
	 */
	LISTENER_OBSERVATION {

		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultKafkaListenerObservationConvention.class;
		}

		@Override
		@NonNull
		public KeyName[] getLowCardinalityKeyNames() {
			return ListenerLowCardinalityTags.values();
		}

		@Override
		@NonNull
		public KeyName[] getHighCardinalityKeyNames() {
			return ListenerHighCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum ListenerLowCardinalityTags implements KeyName {

		/**
		 * Listener id (or listener container bean name).
		 */
		LISTENER_ID {

			@Override
			@NonNull
			public String asString() {
				return "spring.kafka.listener.id";
			}

		},

		/**
		 * Messaging system.
		 * @since 3.2
		 */
		MESSAGING_SYSTEM {

			@Override
			@NonNull
			public String asString() {
				return "messaging.system";
			}

		},

		/**
		 * Messaging operation.
		 * @since 3.2
		 */
		MESSAGING_OPERATION {

			@Override
			@NonNull
			public String asString() {
				return "messaging.operation";
			}

		},

		/**
		 * Messaging source name.
		 * @since 3.2
		 */
		MESSAGING_SOURCE_NAME {

			@Override
			@NonNull
			public String asString() {
				return "messaging.source.name";
			}

		},

		/**
		 * Messaging source kind.
		 * @since 3.2
		 */
		MESSAGING_SOURCE_KIND {

			@Override
			@NonNull
			public String asString() {
				return "messaging.source.kind";
			}

		},

		/**
		 * Messaging the consumer group.
		 * @since 3.2
		 */
		MESSAGING_CONSUMER_GROUP {

			@Override
			@NonNull
			public String asString() {
				return "messaging.kafka.consumer.group";
			}

		},

	}

	/**
	 * High cardinality tags.
	 * @since 3.2
	 */
	public enum ListenerHighCardinalityTags implements KeyName {

		/**
		 * Messaging client id.
		 */
		MESSAGING_CLIENT_ID {

			@Override
			@NonNull
			public String asString() {
				return "messaging.kafka.client_id";
			}

		},

		/**
		 * Messaging consumer id (consumer group and client id).
		 */
		MESSAGING_CONSUMER_ID {

			@Override
			@NonNull
			public String asString() {
				return "messaging.consumer.id";
			}

		},

		/**
		 * Messaging partition.
		 */
		MESSAGING_PARTITION {

			@Override
			@NonNull
			public String asString() {
				return "messaging.kafka.source.partition";
			}

		},

		/**
		 * Messaging message offset.
		 */
		MESSAGING_OFFSET {

			@Override
			@NonNull
			public String asString() {
				return "messaging.kafka.message.offset";
			}

		},

	}

	/**
	 * Default {@link KafkaListenerObservationConvention} for Kafka listener key values.
	 */
	public static class DefaultKafkaListenerObservationConvention implements KafkaListenerObservationConvention {

		/**
		 * A singleton instance of the convention.
		 */
		public static final DefaultKafkaListenerObservationConvention INSTANCE =
				new DefaultKafkaListenerObservationConvention();

		@Override
		@NonNull
		public KeyValues getLowCardinalityKeyValues(KafkaRecordReceiverContext context) {
			String groupId = context.getGroupId();
			KeyValues keyValues = KeyValues.of(
					ListenerLowCardinalityTags.LISTENER_ID.withValue(context.getListenerId()),
					ListenerLowCardinalityTags.MESSAGING_SYSTEM.withValue("kafka"),
					ListenerLowCardinalityTags.MESSAGING_OPERATION.withValue("process"),
					ListenerLowCardinalityTags.MESSAGING_SOURCE_NAME.withValue(context.getSource()),
					ListenerLowCardinalityTags.MESSAGING_SOURCE_KIND.withValue("topic")
			);

			if (StringUtils.hasText(groupId)) {
				keyValues = keyValues
						.and(ListenerLowCardinalityTags.MESSAGING_CONSUMER_GROUP.withValue(groupId));
			}

			return keyValues;
		}

		@Override
		@NonNull
		public KeyValues getHighCardinalityKeyValues(KafkaRecordReceiverContext context) {
			String clientId = context.getClientId();
			String consumerId = getConsumerId(context.getGroupId(), clientId);
			KeyValues keyValues = KeyValues.of(
					ListenerHighCardinalityTags.MESSAGING_PARTITION.withValue(context.getPartition()),
					ListenerHighCardinalityTags.MESSAGING_OFFSET.withValue(context.getOffset())
			);

			if (StringUtils.hasText(clientId)) {
				keyValues = keyValues
						.and(ListenerHighCardinalityTags.MESSAGING_CLIENT_ID.withValue(clientId));
			}

			if (StringUtils.hasText(consumerId)) {
				keyValues = keyValues
						.and(ListenerHighCardinalityTags.MESSAGING_CONSUMER_ID.withValue(consumerId));
			}

			return keyValues;
		}

		@Override
		public String getContextualName(KafkaRecordReceiverContext context) {
			return context.getSource() + " process";
		}

		private static @Nullable String getConsumerId(@Nullable String groupId, @Nullable String clientId) {
			if (StringUtils.hasText(groupId)) {
				if (StringUtils.hasText(clientId)) {
					return groupId + " - " + clientId;
				}
				return groupId;
			}
			return clientId;
		}

	}

}
