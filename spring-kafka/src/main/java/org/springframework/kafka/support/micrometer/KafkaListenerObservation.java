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

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.DocumentedObservation;

/**
 * Spring for Apache Kafka Observation for listeners.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public enum KafkaListenerObservation implements DocumentedObservation {

	/**
	 * Observation for Kafka listeners.
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
		 * Listener id.
		 */
		LISTENER_ID {

			@Override
			public String asString() {
				return "spring.kafka.listener.id";
			}

		}

	}

}
