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

package org.springframework.kafka.aot;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Detect and register Avro types for Apache Kafka listeners.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class KafkaAvroBeanRegistrationAotProcessor implements BeanRegistrationAotProcessor {

	private static final String CONSUMER_RECORD_CLASS_NAME = ConsumerRecord.class.getName();

	private static final String CONSUMER_RECORDS_CLASS_NAME = ConsumerRecord.class.getName();

	private static final String AVRO_GENERATED_CLASS_NAME = "org.apache.avro.specific.AvroGenerated";

	private static final boolean AVRO_PRESENT = ClassUtils.isPresent(AVRO_GENERATED_CLASS_NAME, null);

	@Override
	@Nullable
	public BeanRegistrationAotContribution processAheadOfTime(RegisteredBean registeredBean) {
		if (!AVRO_PRESENT) {
			return null;
		}
		Class<?> beanType = registeredBean.getBeanClass();
		if (!isListener(beanType)) {
			return null;
		}
		Set<Class<?>> avroTypes = new HashSet<>();
		if (GenericMessageListener.class.isAssignableFrom(beanType)) {
			ReflectionUtils.doWithMethods(beanType, method -> {
				Type[] types = method.getGenericParameterTypes();
				if (types.length > 0) {
					ResolvableType resolvableType = ResolvableType.forType(types[0]);
					if (List.class.equals(resolvableType.getRawClass())) {
						resolvableType = resolvableType.getGeneric(0);
					}
					Class<?> keyType = resolvableType.resolveGeneric(0);
					Class<?> valueType = resolvableType.resolveGeneric(1);
					checkType(keyType, avroTypes);
					checkType(valueType, avroTypes);
				}
			}, method -> method.getName().equals("onMessage"));
		}
		if (avroTypes.size() > 0) {
			return (generationContext, beanRegistrationCode) -> {
				ReflectionHints reflectionHints = generationContext.getRuntimeHints().reflection();
				avroTypes.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
								MemberCategory.INVOKE_PUBLIC_METHODS)));
			};
		}
		return null;
	}

	private static boolean isListener(Class<?> beanType) {
		return GenericMessageListener.class.isAssignableFrom(beanType);
	}

	private static void checkType(@Nullable Type paramType, Set<Class<?>> avroTypes) {
		if (paramType == null) {
			return;
		}
		boolean container = isContainer(paramType);
		if (!container && paramType instanceof Class) {
			MergedAnnotations mergedAnnotations = MergedAnnotations.from((Class<?>) paramType);
			if (mergedAnnotations.isPresent(AVRO_GENERATED_CLASS_NAME)) {
				avroTypes.add((Class<?>) paramType);
			}
		}
		else if (container) {
			if (paramType instanceof ParameterizedType) {
				Type[] generics = ((ParameterizedType) paramType).getActualTypeArguments();
				if (generics.length > 0) {
					checkAvro(generics[0], avroTypes);
				}
				if (generics.length == 2) {
					checkAvro(generics[1], avroTypes);
				}
			}
		}
	}

	private static void checkAvro(@Nullable Type generic, Set<Class<?>> avroTypes) {
		if (generic instanceof Class) {
			MergedAnnotations methodAnnotations = MergedAnnotations.from((Class<?>) generic);
			if (methodAnnotations.isPresent(AVRO_GENERATED_CLASS_NAME)) {
				avroTypes.add((Class<?>) generic);
			}
		}
	}

	private static boolean isContainer(Type paramType) {
		if (paramType instanceof ParameterizedType) {
			Type rawType = ((ParameterizedType) paramType).getRawType();
			return (rawType.equals(List.class))
					|| rawType.getTypeName().equals(CONSUMER_RECORD_CLASS_NAME)
					|| rawType.getTypeName().equals(CONSUMER_RECORDS_CLASS_NAME);
		}
		return false;
	}

}
