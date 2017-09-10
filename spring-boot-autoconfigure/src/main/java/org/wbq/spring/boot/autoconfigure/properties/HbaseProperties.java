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

import org.springframework.boot.bind.RelaxedPropertyResolver;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import java.io.File;

@ConfigurationProperties(prefix = "hbase")
public class HbaseProperties
        implements EnvironmentAware {
    private Environment environment;

    private final String HBASE_PREFIX = "hbase.";

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    private String hbaseHome() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, HBASE_PREFIX);
        return resolver.getProperty("HBASE_HOME");
    }

    private File configureFilePath() {
        String hbaseHome = hbaseHome();
        if (hbaseHome == null) return null;
        else {
            File hbaseHomeDic = new File(hbaseHome);
            if (hbaseHomeDic.exists() && hbaseHomeDic.isDirectory()) {
                File configPath = new File(hbaseHomeDic, "/conf/");
                if (configPath.exists() && configPath.isDirectory()) {
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
                configuration.addResource(propFile.toURI().toURL());
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
        return resolver.getRequiredProperty("hbase.zookeeper.quorum");
    }
}
