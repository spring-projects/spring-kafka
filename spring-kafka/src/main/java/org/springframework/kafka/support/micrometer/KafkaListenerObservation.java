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

import io.micrometer.common.docs.TagKey;
import io.micrometer.observation.docs.DocumentedObservation;

/**
 * Spring Kafka Observation for listeners.
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
		public String getName() {
			return "spring.kafka.listener";
		}

		@Override
		public String getContextualName() {
			return "KafkaTemplate Observation";
		}

		@Override
		public TagKey[] getLowCardinalityTagKeys() {
			return ListenerLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum ListenerLowCardinalityTags implements TagKey {

		/**
		 * Listener id.
		 */
		LISTENER_ID {

			@Override
			public String getKey() {
				return "listener.id";
			}

		}

	}

}
