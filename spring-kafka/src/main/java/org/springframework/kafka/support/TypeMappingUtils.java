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

package org.springframework.kafka.support;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.kafka.annotation.TypeMapping;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * TODO: Move to spring-messaging?
 *
 * Utilities for type mappings.
 *
 * @author Gary Russell
 * @since 2.6
 *
 */
public final class TypeMappingUtils {

	private TypeMappingUtils() {
	}

	/**
	 * Find {@link TypeMapping} annotations on classes in the provided packages and return
	 * as a map of typeid:class.
	 * @param context the application context.
	 * @param packagesToScan the packages to scan.
	 * @return the mappings.
	 */
	public static Map<String, Class<?>> findTypeMappings(ApplicationContext context, String... packagesToScan) {

		Map<String, Class<?>> mappings = new HashMap<>();
		Set<Class<?>> candidates = new HashSet<>();
		ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
		scanner.setEnvironment(context.getEnvironment());
		scanner.setResourceLoader(context);
		scanner.addIncludeFilter(new AnnotationTypeFilter(TypeMapping.class));
		for (String packageToScan : packagesToScan) {
			if (StringUtils.hasText(packageToScan)) {
				for (BeanDefinition candidate : scanner.findCandidateComponents(packageToScan)) {
					try {
						candidates.add(ClassUtils.forName(candidate.getBeanClassName(), context.getClassLoader()));
					}
					catch (ClassNotFoundException | LinkageError e) {
						throw new IllegalStateException(e);
					}
				}
			}
		}
		for (Class<?> candidate : candidates) {
			AnnotationAttributes annotationAttributes =
					AnnotatedElementUtils.findMergedAnnotationAttributes(candidate, TypeMapping.class, false, false);
			for (String  value : annotationAttributes.getStringArray("value")) {
				if (StringUtils.hasText(value)) {
					mappings.put(value, candidate);
				}
			}
		}
		return mappings;
	}

}
