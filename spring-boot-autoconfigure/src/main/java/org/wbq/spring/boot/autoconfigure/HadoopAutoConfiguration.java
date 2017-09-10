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

import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.wbq.spring.boot.autoconfigure.hadoop.HdfsTool;
import org.wbq.spring.boot.autoconfigure.properties.HadoopProperities;

import java.io.IOException;

@Configuration
@ConditionalOnClass({ FileSystem.class, org.apache.hadoop.conf.Configuration.class })
@EnableConfigurationProperties(HadoopProperities.class)
public class HadoopAutoConfiguration {


    @Bean
    @ConditionalOnMissingBean
    public org.apache.hadoop.conf.Configuration configuration(HadoopProperities hadoopProperities){
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        hadoopProperities.loadDetectedConfiguration(configuration);
        return configuration;
    }

    @Bean
    @ConditionalOnMissingBean
    public HdfsTool hadoopTools(org.apache.hadoop.conf.Configuration configuration, HadoopProperities hadoopProperities) throws IOException{
        return new HdfsTool(configuration, hadoopProperities);
    }
}
