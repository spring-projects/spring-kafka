/*
 * Copyright 2022-2023 the original author or authors.
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
import org.springframework.util.StringUtils;

/**
 * Spring for Apache Kafka Observation for listeners.
 *
 * @author Gary Russell
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
		public String getPrefix() {
			return "spring.kafka.listener";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return ListenerLowCardinalityTags.values();
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
			public String asString() {
				return "spring.kafka.listener.id";
			}

		},

		/**
		 * Messaging system
		 */
		MESSAGING_SYSTEM {

			@Override
			public String asString() {
				return "messaging.system";
			}

		},

		/**
		 * Messaging operation
		 */
		MESSAGING_OPERATION {

			@Override
			public String asString() {
				return "messaging.operation";
			}

		},

		/**
		 * Messaging consumer id
		 */
		MESSAGING_CONSUMER_ID {

			@Override
			public String asString() {
				return "messaging.consumer.id";
			}

		},

		/**
		 * Messaging source name
		 */
		MESSAGING_SOURCE_NAME {

			@Override
			public String asString() {
				return "messaging.source.name";
			}

		},

		/**
		 * Messaging source kind
		 */
		MESSAGING_SOURCE_KIND {

			@Override
			public String asString() {
				return "messaging.source.kind";
			}

		},

		/**
		 * Messaging consumer group
		 */
		MESSAGING_CONSUMER_GROUP {

			@Override
			public String asString() {
				return "messaging.kafka.consumer.group";
			}

		},

		/**
		 * Messaging client id
		 */
		MESSAGING_CLIENT_ID {

			@Override
			public String asString() {
				return "messaging.kafka.client_id";
			}

		},

		/**
		 * Messaging partition
		 */
		MESSAGING_PARTITION {

			@Override
			public String asString() {
				return "messaging.kafka.partition";
			}

		},

		/**
		 * Messaging message offset
		 */
		MESSAGING_OFFSET {

			@Override
			public String asString() {
				return "messaging.kafka.message.offset";
			}

		}

	}

	/**
	 * Default {@link KafkaListenerObservationConvention} for Kafka listener key values.
	 *
	 * @author Gary Russell
	 * @since 3.0
	 *
	 */
	public static class DefaultKafkaListenerObservationConvention implements KafkaListenerObservationConvention {

		/**
		 * A singleton instance of the convention.
		 */
		public static final DefaultKafkaListenerObservationConvention INSTANCE =
				new DefaultKafkaListenerObservationConvention();

		@Override
		public KeyValues getLowCardinalityKeyValues(KafkaRecordReceiverContext context) {
			KeyValues keyValues = KeyValues.of(
					ListenerLowCardinalityTags.LISTENER_ID.withValue(context.getListenerId()),
					ListenerLowCardinalityTags.MESSAGING_CONSUMER_ID.withValue(getConsumerId(context)),
					ListenerLowCardinalityTags.MESSAGING_SYSTEM.withValue("kafka"),
					ListenerLowCardinalityTags.MESSAGING_OPERATION.withValue("receive"),
					ListenerLowCardinalityTags.MESSAGING_SOURCE_NAME.withValue(context.getSource()),
					ListenerLowCardinalityTags.MESSAGING_SOURCE_KIND.withValue("topic"),
					ListenerLowCardinalityTags.MESSAGING_PARTITION.withValue(context.getPartition()),
					ListenerLowCardinalityTags.MESSAGING_OFFSET.withValue(context.getOffset()),
					ListenerLowCardinalityTags.MESSAGING_CONSUMER_GROUP.withValue(context.getGroupId())
			);

			if (StringUtils.hasText(context.getClientId())) {
				keyValues = keyValues.and(ListenerLowCardinalityTags.MESSAGING_CLIENT_ID.withValue(context.getClientId()));
			}

			return keyValues;
		}

		@Override
		public String getContextualName(KafkaRecordReceiverContext context) {
			return context.getSource() + " receive";
		}

		@Override
		public String getName() {
			return "spring.kafka.listener";
		}

		private String getConsumerId(KafkaRecordReceiverContext context) {
			if (StringUtils.hasText(context.getClientId())) {
				return context.getGroupId() + " - " + context.getClientId();
			}
			return context.getGroupId();
		}

	}

}
