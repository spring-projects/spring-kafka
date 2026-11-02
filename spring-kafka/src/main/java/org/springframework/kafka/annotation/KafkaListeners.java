/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Container annotation that aggregates several {@link KafkaListener} annotations.
 * <p>
 * Can be used natively, declaring several nested {@link KafkaListener} annotations.
 * Can also be used in conjunction with Java 8's support for repeatable annotations,
 * where {@link KafkaListener} can simply be declared several times on the same method
 * (or class), implicitly generating this container annotation.
 *
 * @author Gary Russell
 *
 * @see KafkaListener
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaListeners {

	KafkaListener[] value();

}
