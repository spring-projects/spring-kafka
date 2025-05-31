/*
 * Copyright 2016-2024 the original author or authors.
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

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;
import org.springframework.kafka.entity.reactive.KafkaEntityDefaultConfiguration;
import org.springframework.kafka.entity.reactive.KafkaEntityProcessor;
import org.springframework.kafka.entity.reactive.KafkaEntityPublisher;
import org.springframework.kafka.entity.reactive.KafkaEntitySubscriber;

/**
 * Enable Kafka Entities annotated fields. To be used on
 * {@link org.springframework.context.annotation.Configuration Configuration} classes.
 *
 * Note that all fields in beans annotated with  fields in Beans annotated
 * with @{@link KafkaEntityProcessor} @{@link KafkaEntitySubscriber} @{@link KafkaEntityPublisher} will be detected.
 *
 * @author Popovics Boglarka
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KafkaEntityDefaultConfiguration.class)
public @interface EnableKafkaEntity {
}
