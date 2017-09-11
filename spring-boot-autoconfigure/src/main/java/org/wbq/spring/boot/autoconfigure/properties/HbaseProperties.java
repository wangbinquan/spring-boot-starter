/*
 *
 * Copyright BinquanWang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.wbq.spring.boot.autoconfigure.properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.bind.RelaxedPropertyResolver;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import java.io.File;
import java.net.URL;

@ConfigurationProperties(prefix = "hbase")
public class HbaseProperties
        implements EnvironmentAware {
    private Log LOG = LogFactory.getLog(getClass());
    private Environment environment;
    private final String HBASE_PREFIX = "hbase.";

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    private String hbaseHome() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, HBASE_PREFIX);
        String hbaseHome = resolver.getProperty("HBASE_HOME");
        LOG.info("Resolve HBASE_HOME at: [" + hbaseHome + "]");
        return hbaseHome;
    }

    private File configureFilePath() {
        String hbaseHome = hbaseHome();
        if (hbaseHome == null) return null;
        else {
            File hbaseHomeDic = new File(hbaseHome);
            if (hbaseHomeDic.exists() && hbaseHomeDic.isDirectory()) {
                File configPath = new File(hbaseHomeDic, "/conf/");
                if (configPath.exists() && configPath.isDirectory()) {
                    LOG.info("Hbase config directory exist: [" + configPath.getAbsolutePath() + "]");
                    return configPath;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    private void loadFile(org.apache.hadoop.conf.Configuration configuration, File confPath, String fileName) {
        File propFile = new File(confPath, fileName);
        if (propFile.exists() && propFile.isFile()) {
            try {
                URL uri = propFile.toURI().toURL();
                configuration.addResource(uri);
                LOG.info("Set hbase properities file: [" + uri.toString() + "]");
            } catch (Exception e) {
                //search in classpath
                configuration.addResource(fileName);
            }
        }
    }

    public void loadDetectedConfiguration(org.apache.hadoop.conf.Configuration configuration) {
        File configureFilePath = configureFilePath();
        if (configureFilePath != null) {
            loadFile(configuration, configureFilePath, "hbase-default.xml");
            loadFile(configuration, configureFilePath, "hbase-site.xml");
        } else {
            configuration.addResource("hbase-default.xml");
            configuration.addResource("hbase-site.xml");
        }
    }

    public String getZookeeperQuorum() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, HBASE_PREFIX);
        String zookeeperQuorum = resolver.getRequiredProperty("zookeeper.quorum");
        LOG.info("Resolve hbase.zookeeper.quorum = [" + zookeeperQuorum + "] in spring properities");
        return zookeeperQuorum;
    }
}
