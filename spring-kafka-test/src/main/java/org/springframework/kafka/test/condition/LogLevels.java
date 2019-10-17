/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.test.condition;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test classes annotated with this will change logging levels between tests. It can also
 * be applied to individual test methods. If both class-level and method-level annotations
 * are present, the method-level annotation is used.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
@ExtendWith(LogLevelsCondition.class)
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LogLevels {

	/**
	 * Classes representing Log4j categories to change.
	 * @return the classes.
	 */
	Class<?>[] classes() default {};

	/**
	 * Category names representing Log4j or Logback categories to change.
	 * @return the names.
	 */
	String[] categories() default {};

	/**
	 * The Log4j level name to switch the categories to during the test.
	 * @return the level (default Log4j {@code Levels.toLevel()} - currently, DEBUG).
	 */
	String level() default "";

}
