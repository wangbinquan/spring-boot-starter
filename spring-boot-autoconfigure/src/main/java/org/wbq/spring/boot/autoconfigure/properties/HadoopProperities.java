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
import java.net.URI;
import java.net.URL;

@ConfigurationProperties(prefix = "hadoop")
public class HadoopProperities
        implements EnvironmentAware {
    private Log LOG = LogFactory.getLog(getClass());
    private Environment environment;
    private final String HADOOP_PREFIX = "hadoop.";

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    private String hadoopHome() {
        String hadoopHome = environment.getProperty("HADOOP_HOME");
        LOG.info("Resolve HADOOP_HOME at: [" + hadoopHome + "]");
        return hadoopHome;
    }

    private File configureFilePath() {
        String hadoopHome = hadoopHome();
        if (hadoopHome == null) return null;
        else {
            File hadoopHomeDic = new File(hadoopHome);
            if (hadoopHomeDic.exists() && hadoopHomeDic.isDirectory()) {
                File configPath = new File(hadoopHomeDic, "/etc/hadoop/");
                if (configPath.exists() && configPath.isDirectory()) {
                    LOG.info("Hadoop config directory exist: [" + configPath.getAbsolutePath() + "]");
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
                LOG.info("Set hadoop properities file: [" + uri.toString() + "]");
            } catch (Exception e) {
                //search in classpath
                configuration.addResource(fileName);
            }
        }
    }

    public void loadDetectedConfiguration(org.apache.hadoop.conf.Configuration configuration) {
        File configureFilePath = configureFilePath();
        if (configureFilePath != null) {
            loadFile(configuration, configureFilePath, "core-default.xml");
            loadFile(configuration, configureFilePath, "core-site.xml");
            loadFile(configuration, configureFilePath, "hdfs-default.xml");
            loadFile(configuration, configureFilePath, "hdfs-site.xml");
            loadFile(configuration, configureFilePath, "hadoop-site.xml");
        } else {
            configuration.addResource("core-default.xml");
            configuration.addResource("core-site.xml");
            configuration.addResource("hdfs-default.xml");
            configuration.addResource("hdfs-site.xml");
            configuration.addResource("hadoop-site.xml");
        }
    }

    public String hadoopHdfsUri() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, HADOOP_PREFIX);
        String hdfsUri = resolver.getProperty("hdfs.uri");
        LOG.info("Resolve hadoop.hdfs.uri = [" + hdfsUri + "] in spring properities");
        return hdfsUri;
    }
}
