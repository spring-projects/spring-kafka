/*
 * Copyright 2026-present the original author or authors.
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

package com.example.sample09;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import org.springframework.beans.factory.InitializingBean;

/**
 * Sets {@code share.auto.offset.reset=earliest} on the share group. Share groups do not honour
 * the consumer {@code auto.offset.reset} property; the equivalent is a group configuration that
 * has to be applied with an {@link Admin} client. Singletons are instantiated before listener
 * containers are started, so the group is configured before the share consumer first subscribes.
 *
 * @author Omar Morales Ortega
 *
 * @since 4.2
 */
public class ShareGroupInitializer implements InitializingBean {

	private final List<String> bootstrapServers;

	private final String groupId;

	public ShareGroupInitializer(List<String> bootstrapServers, String groupId) {
		this.bootstrapServers = bootstrapServers;
		this.groupId = groupId;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		try (Admin admin = Admin.create(adminProps)) {
			ConfigResource group = new ConfigResource(ConfigResource.Type.GROUP, this.groupId);
			Collection<AlterConfigOp> ops = List.of(
					new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"),
							AlterConfigOp.OpType.SET));
			admin.incrementalAlterConfigs(Map.of(group, ops)).all().get();
		}
	}

}
