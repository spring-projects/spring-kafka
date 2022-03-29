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
 * Spring Kafka Observation for {@link org.springframework.kafka.core.KafkaTemplate}.
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
		public String getName() {
			return "spring.kafka.template";
		}

		@Override
		public String getContextualName() {
			return "KafkaTemplate Observation";
		}

		@Override
		public TagKey[] getLowCardinalityTagKeys() {
			return TemplateLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum TemplateLowCardinalityTags implements TagKey {

		/**
		 * Bean name of the template.
		 */
		BEAN_NAME {

			@Override
			public String getKey() {
				return "bean.name";
			}

		}

	}

}
