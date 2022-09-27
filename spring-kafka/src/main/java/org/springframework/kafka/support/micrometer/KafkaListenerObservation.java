/*
 * Copyright 2022 the original author or authors.
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
			return KeyValues.of(KafkaListenerObservation.ListenerLowCardinalityTags.LISTENER_ID.asString(),
							context.getListenerId());
		}

		@Override
		public String getContextualName(KafkaRecordReceiverContext context) {
			return context.getSource() + " receive";
		}

		@Override
		public String getName() {
			return "spring.kafka.listener";
		}

	}

}
