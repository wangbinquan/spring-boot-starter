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

package org.wbq.spring.boot.autoconfigure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.wbq.spring.boot.autoconfigure.hbase.HbaseTool;
import org.wbq.spring.boot.autoconfigure.properties.HbaseProperties;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Configuration
@ConditionalOnClass({HBaseConfiguration.class})
@ConditionalOnProperty(prefix = "hbase.zookeeper", name = "quorum")
@ConditionalOnBean({org.apache.hadoop.conf.Configuration.class})
@AutoConfigureAfter({HadoopAutoConfiguration.class})
@EnableConfigurationProperties(HbaseProperties.class)
public class HbaseAutoConfiguration {
    private Log LOG = LogFactory.getLog(getClass());
    private org.apache.hadoop.conf.Configuration configuration = null;

    public HbaseAutoConfiguration(HbaseProperties hbaseProperties, org.apache.hadoop.conf.Configuration configuration) {
        configuration.setClassLoader(HBaseConfiguration.class.getClassLoader());
//        checkDefaultsVersion(configuration);
        HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(configuration);
        if (configuration.get("hbase.zookeeper.quorum") == null) {
            String zookeeperQuorum = hbaseProperties.getZookeeperQuorum();
            Assert.isTrue(zookeeperQuorum != null, "hbase.zookeeper.quorum not found in properities");
            configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
            LOG.info("Set hbase configuration hbase.zookeeper.quorum = [" + zookeeperQuorum + "]");
        }
        this.configuration = configuration;
    }

    private static void checkDefaultsVersion(org.apache.hadoop.conf.Configuration conf) {
        if (conf.getBoolean("hbase.defaults.for.version.skip", Boolean.FALSE)) return;
        String defaultsVersion = conf.get("hbase.defaults.for.version");
        String thisVersion = VersionInfo.getVersion();
        if (!thisVersion.equals(defaultsVersion)) {
            throw new RuntimeException(
                    "hbase-default.xml file seems to be for an older version of HBase (" +
                            defaultsVersion + "), this version is " + thisVersion);
        }
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(org.apache.hadoop.conf.Configuration.class)
    public HbaseTool hbaseTool() throws IOException {
        LOG.info("Gen HbaseTool class");
        return new HbaseTool(configuration);
    }


}
