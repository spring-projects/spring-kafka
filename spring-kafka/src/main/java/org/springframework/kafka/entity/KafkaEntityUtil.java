/*
 * Copyright 2016-2025 the original author or authors.
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

package org.springframework.kafka.entity;

import io.micrometer.common.util.StringUtils;

/**
 * Kafka Entity Utility Class.
 *
 * @author Popovics Boglarka
 */
public final class KafkaEntityUtil {

	private KafkaEntityUtil() {
		super();
	}

	/**
	 * Gets the name of the topic to a @KafkaEntity.
	 *
	 * @param entity class which is marked with @KafkaEntity
	 *
	 * @return the name of the entity class with included packagename but you can
	 *         use custom Topic name like
	 *         this @KafkaEntity(customTopicName="PRODUCT")
	 */
	public static String getTopicName(Class<?> entity) {
		KafkaEntity topic = extractKafkaEntity(entity);
		return getTopicName(entity, topic);
	}

	private static String getTopicName(Class<?> entity, KafkaEntity topic) {
		if (!StringUtils.isEmpty(topic.customTopicName())) {
			return topic.customTopicName();
		}
		return entity.getName();
	}

	private static KafkaEntity extractKafkaEntity(Class<?> entity) {
		return (KafkaEntity) entity.getAnnotation(KafkaEntity.class);
	}
}
