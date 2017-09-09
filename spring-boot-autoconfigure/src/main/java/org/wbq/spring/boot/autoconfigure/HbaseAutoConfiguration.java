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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.wbq.spring.boot.autoconfigure.properties.HbaseProperties;

import javax.annotation.PostConstruct;

@Configuration
@ConditionalOnClass({ HBaseConfiguration.class })
@ConditionalOnProperty(prefix = "hbase.zookeeper", name = "quorum")
@ConditionalOnBean({ org.apache.hadoop.conf.Configuration.class })
@AutoConfigureAfter( {HadoopAutoConfiguration.class} )
@EnableConfigurationProperties(HbaseProperties.class)
public class HbaseAutoConfiguration {
    @PostConstruct
    private void applyZookeeper(HbaseProperties hbaseProperties, org.apache.hadoop.conf.Configuration configuration){
        configuration.set("hbase.zookeeper.quorum", hbaseProperties.getZookeeperQuorum());
    }
}