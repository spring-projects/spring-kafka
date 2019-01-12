/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.test.TestUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.net.ServerSocket;
import java.util.Properties;

/**
 * A zookeeper server to use with unit tests.
 *
 * @author Dmitrii Apanasevich
 */
public final class ZooKeeperServer {

  private static final String ZOO_CLIENT_PORT_PROPERTY = "clientPort";

  private static final String ZOO_DATA_DIR_PROPERTY = "dataDir";

  private static final String ZOO_PREFIX = "zoo-";

  private static final Log logger = LogFactory.getLog(ZooKeeperServer.class);

  private final ZooKeeperShutdownable zookeeper;

  private final ServerConfig serverConfig;

  private final String host;

  public ZooKeeperServer(String host) {
    final QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
    try (final ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      final int port = socket.getLocalPort();
      final Properties zooKeeperConfig = new Properties();
      zooKeeperConfig.setProperty(ZOO_DATA_DIR_PROPERTY, TestUtils.tempDirectory(ZOO_PREFIX).getPath());
      zooKeeperConfig.setProperty(ZOO_CLIENT_PORT_PROPERTY, String.valueOf(port));
      quorumPeerConfig.parseProperties(zooKeeperConfig);
    } catch (Throwable t) {
      logger.error("Couldn't start zookeeper", t);
      org.junit.Assert.fail(t.getMessage());
    }
    this.host = host;
    this.serverConfig = new ServerConfig();
    serverConfig.readFrom(quorumPeerConfig);

    this.zookeeper = new ZooKeeperShutdownable();
  }

  public void startup() {
    new Thread(() -> {
      try {
        zookeeper.runFromConfig(serverConfig);
      } catch (Throwable t) {
        logger.error("Couldn't start zookeeper", t);
        org.junit.Assert.fail(t.getMessage());
      }
    }).start();
  }

  public void shutdown() {
    this.zookeeper.shutdown();
  }

  public String zkAddress() {
    return host + ':' + serverConfig.getClientPortAddress().getPort();
  }

  /**
   * A wrapper around ZooKeeperServerMain to have an ability to shutdown it.
   *
   * @author Dmitrii Apanasevich
   */
  private static final class ZooKeeperShutdownable extends ZooKeeperServerMain {
    @Override
    public void shutdown() {
      super.shutdown();
    }
  }
}
