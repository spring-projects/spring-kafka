/*
 * Copyright 2014-2024 the original author or authors.
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

package org.springframework.kafka.config;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.kafka.annotation.EnableParallelConsumer;
/**
 * ParallelConsumerImportSelector is to register {@link ParallelConsumerConfiguration}.
 * If you want to import {@link ParallelConsumerConfiguration} to your application,
 * you just annotated {@link EnableParallelConsumer} to your spring application.
 * @author ...
 * @since 3.2.0
 */

public class ParallelConsumerImportSelector implements ImportSelector {
	@Override
	public String[] selectImports(AnnotationMetadata importingClassMetadata) {
		return new String[]{ParallelConsumerConfiguration.class.getName()};
	}
}