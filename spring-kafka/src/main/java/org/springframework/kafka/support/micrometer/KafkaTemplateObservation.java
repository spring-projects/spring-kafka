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

/**
 * Spring for Apache Kafka Observation for
 * {@link org.springframework.kafka.core.KafkaTemplate}.
 *
 * @author Gary Russell
 * @author Christian Mergenthaler
 * @author Wang Zhiyang
 *
 * @since 3.0
 *
 */
public enum KafkaTemplateObservation implements ObservationDocumentation {

	/**
	 * Observation for KafkaTemplates.
	 */
	TEMPLATE_OBSERVATION {

		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultKafkaTemplateObservationConvention.class;
		}

		@Override
		@NonNull
		public KeyName[] getLowCardinalityKeyNames() {
			return TemplateLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 *
	 * @author Christian Mergenthaler
	 * @author Wang Zhiyang
	 *
	 * @since 3.2
	 */
	public enum TemplateLowCardinalityTags implements KeyName {

		/**
		 * Bean name of the template.
		 */
		BEAN_NAME {

			@Override
			@NonNull
			public String asString() {
				return "spring.kafka.template.name";
			}

		},

		/**
		 * Messaging system.
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
		 */
		MESSAGING_OPERATION {

			@Override
			@NonNull
			public String asString() {
				return "messaging.operation";
			}

		},

		/**
		 * Messaging destination name.
		 */
		MESSAGING_DESTINATION_NAME {

			@Override
			@NonNull
			public String asString() {
				return "messaging.destination.name";
			}

		},

		/**
		 * Messaging destination kind.
		 */
		MESSAGING_DESTINATION_KIND {

			@Override
			@NonNull
			public String asString() {
				return "messaging.destination.kind";
			}

		}

	}

	/**
	 * Default {@link KafkaTemplateObservationConvention} for Kafka template key values.
	 *
	 * @author Gary Russell
	 * @author Christian Mergenthaler
	 * @author Wang Zhiyang
	 *
	 * @since 3.0
	 *
	 */
	public static class DefaultKafkaTemplateObservationConvention implements KafkaTemplateObservationConvention {

		/**
		 * A singleton instance of the convention.
		 */
		public static final DefaultKafkaTemplateObservationConvention INSTANCE =
				new DefaultKafkaTemplateObservationConvention();

		@Override
		public KeyValues getLowCardinalityKeyValues(KafkaRecordSenderContext context) {
			return KeyValues.of(
					TemplateLowCardinalityTags.BEAN_NAME.withValue(context.getBeanName()),
					TemplateLowCardinalityTags.MESSAGING_SYSTEM.withValue("kafka"),
					TemplateLowCardinalityTags.MESSAGING_OPERATION.withValue("publish"),
					TemplateLowCardinalityTags.MESSAGING_DESTINATION_KIND.withValue("topic"),
					TemplateLowCardinalityTags.MESSAGING_DESTINATION_NAME.withValue(context.getDestination()));
		}

		@Override
		public String getContextualName(KafkaRecordSenderContext context) {
			return context.getDestination() + " send";
		}

		@Override
		@NonNull
		public String getName() {
			return "spring.kafka.template";
		}

	}

}
