/*
 * Copyright 2020 the original author or authors.
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

/**
 * TODO: Move to spring-messaging?
 *
 * Classes annotated with this can be automatically populated into various type mappers
 * (e.g. Jackson). Several Spring projects, JMS, Kafka, RabbitMQ allow mapping a type id
 * to/from a class, for the purpose of decoupling concrete types used by senders and
 * receivers.
 *
 * @author Gary Russell
 * @since 2.6
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TypeMapping {

	/**
	 * Either a single or multiple type ids to map to/from the annotated class.
	 * @return the type ids.
	 */
	String[] value();

}
