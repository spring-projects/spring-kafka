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

/**
 * Thrown by spring-kafka-extensions library if there is a problem creating a
 * Kafka Entity Bean.
 *
 * @author Popovics Boglarka
 */
public class KafkaEntityException extends Exception {

	private static final long serialVersionUID = 8485773596430780144L;

	/**
	 * The name of the Kafka Entity Bean.
	 */
	private final String beanName;

	/**
	 * Constructs a new exception with the specified message and a name of the Kafka
	 * Entity Bean, which had the problem.
	 *
	 * @param beanName name of the Kafka Entity Bean
	 * @param message  problem bei creating the Kafka Entity Bean
	 */
	public KafkaEntityException(String beanName, String message) {
		super(message);
		this.beanName = beanName;
	}

	/**
	 * Constructs a new exception with the specified cause and a name of the Kafka
	 * Entity Bean, which had the problem.
	 *
	 * @param beanName name of the Kafka Entity Bean
	 * @param e        problem bei creating the Kafka Entity Bean
	 */
	public KafkaEntityException(String beanName, Exception e) {
		super(e);
		this.beanName = beanName;
	}

	/**
	 * Getter to the name of the Kafka Entity Bean, which had the problem.
	 *
	 * @return name of the Kafka Entity Bean
	 */
	public String getBeanName() {
		return this.beanName;
	}

	@Override
	public String getMessage() {
		return this.beanName + ": " + super.getMessage();
	}

}
