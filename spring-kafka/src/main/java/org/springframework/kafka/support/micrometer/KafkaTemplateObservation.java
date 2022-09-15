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
 * Spring for Apache Kafka Observation for
 * {@link org.springframework.kafka.core.KafkaTemplate}.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public enum KafkaTemplateObservation implements DocumentedObservation {

	/**
	 * {@link org.springframework.kafka.core.KafkaTemplate} observation.
	 */
	TEMPLATE_OBSERVATION {

		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultKafkaTemplateObservationConvention.class;
		}

		@Override
		public String getPrefix() {
			return "spring.kafka.template";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return TemplateLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum TemplateLowCardinalityTags implements KeyName {

		/**
		 * Bean name of the template.
		 */
		BEAN_NAME {

			@Override
			public String asString() {
				return "spring.kafka.template.name";
			}

		}

	}

}
