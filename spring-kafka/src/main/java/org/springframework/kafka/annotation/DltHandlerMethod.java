/*
 * Copyright 2018-2024 the original author or authors.
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

import org.springframework.kafka.support.EndpointHandlerMethod;

/**
 * Annotation to configure the {@link org.springframework.kafka.support.EndpointHandlerMethod} for handling the DLT,
 * as part of {@link RetryableTopic} annotation configuration.
 * This is equivalent to the builder way of {@link org.springframework.kafka.retrytopic.RetryTopicConfigurer}
 * configuration via {@link org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder#dltHandlerMethod(EndpointHandlerMethod)}.
 *
 * @author Joo Hyuk Kim
 *
 * @since 3.2
 *
 * @see org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder#dltHandlerMethod(String, String)
 *
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DltHandlerMethod {

	/**
	 * The bean name of the DLT handler method.
	 *
	 * @return the bean name.
	 */
	String beanName() default "";

	/**
	 * The method name of the DLT handler method.
	 *
	 * @return the method name.
	 */
	String methodName() default "";

}
