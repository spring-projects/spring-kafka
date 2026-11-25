/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.retrytopic;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * @author tomazlemos
 * @since 14/02/21
 */
public abstract class TestClockUtils {

	public static Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(325587600000L), ZoneId.systemDefault());

}
