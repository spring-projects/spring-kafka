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

package org.springframework.kafka.annotation;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.Set;

import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodFilter;

/**
 * Find the appropriate handler method when the target class has a class-level {@link KafkaListener}
 * annotation and contains a single public method without a {@link KafkaHandler} annotation.
 *
 * @author Sanghyeok An
 *
 * @since 4.0
 *
 * @see KafkaListenerAnnotationBeanPostProcessor
 */
public class KafkaHandlerParser {

	/**
	 * Finds the appropriate handler method when the target class has a class-level {@link KafkaListener}
	 * annotation and contains a single public method without a {@link KafkaHandler} annotation.
	 * This method is used to determine which method should be invoked for Kafka message handling
	 * when no explicit {@link KafkaHandler} annotations are present but the class itself is annotated with {@link KafkaListener}.
	 *
	 * @param targetClass the class to inspect for handler methods
	 * @return the method to be used for kafka message handling, or {@code null} if no suitable method is found.
	 */
	public Optional<Method> parseSingleHandlerMethod(Class<?> targetClass) {

		Set<Method> methodsWithAnnotations = MethodIntrospector.selectMethods(
				targetClass, (MethodFilter) method ->
						AnnotatedElementUtils.findMergedAnnotation(method, KafkaHandler.class) != null ||
						AnnotatedElementUtils.findMergedAnnotation(method, KafkaListener.class) != null
		);

		if (!methodsWithAnnotations.isEmpty()) {
			return Optional.empty();
		}

		Set<Method> publicMethodsWithoutHandlerAnnotation = MethodIntrospector.selectMethods(
				targetClass, (ReflectionUtils.MethodFilter) method ->
						Modifier.isPublic(method.getModifiers()) &&
						AnnotatedElementUtils.findMergedAnnotation(method, KafkaHandler.class) == null &&
						AnnotatedElementUtils.findMergedAnnotation(method, KafkaListener.class) == null
		);

		if (!hasSinglePublicMethod(publicMethodsWithoutHandlerAnnotation)) {
			return Optional.empty();
		}

		Method publicMethod = publicMethodsWithoutHandlerAnnotation.iterator().next();
		return Optional.of(publicMethod);
	}

	private boolean hasSinglePublicMethod(Set<Method> methods) {
		return methods.size() == 1;
	}

}
