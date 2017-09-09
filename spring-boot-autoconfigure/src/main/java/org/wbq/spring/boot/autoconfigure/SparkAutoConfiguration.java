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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.wbq.spring.boot.autoconfigure.properties.SparkProperties;

import java.util.Map;

@Configuration
@ConditionalOnClass({SparkContext.class, SparkSession.class})
@ConditionalOnProperty(prefix = "spark", name = "master")
@EnableConfigurationProperties(SparkProperties.class)
public class SparkAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SparkSession sparkSession(SparkProperties sparkProperties) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(sparkProperties.getMaster())
                .setAppName(sparkProperties.getAppName());
        for (Map.Entry<String, String> entry: sparkProperties.getOtherSparkConf().entrySet()){
            sparkConf.set(entry.getKey(), entry.getValue());
        }
        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    @Bean
    @ConditionalOnMissingBean
    public SparkContext sparkContext(SparkSession sparkSession){
        return sparkSession.sparkContext();
    }
}
