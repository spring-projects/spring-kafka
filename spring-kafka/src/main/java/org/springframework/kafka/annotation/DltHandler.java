/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.annotation;


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to determine the method that should process the DLT topic message.
 * The method can have the same parameters as a {@link KafkaListener} method can have (Message, Acknowledgement, etc).
 *
 * <p>
 * The annotated method must be in the same class as the corresponding {@link KafkaListener} annotation.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DltHandler {

}
