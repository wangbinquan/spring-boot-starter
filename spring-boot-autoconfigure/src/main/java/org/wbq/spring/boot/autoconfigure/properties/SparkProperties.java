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

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "spark")
public class SparkProperties
        implements EnvironmentAware {
    private Log LOG = LogFactory.getLog(getClass());
    private Environment environment;
    private final String SPARK_PREFIX = "spark.";

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public String getSparkGeneratorType() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, "");
        String sparkGeneratorType = resolver.getRequiredProperty("generator.spark.type");
        LOG.info("Resolve generator.spark.type = [" + sparkGeneratorType + "]");
        return sparkGeneratorType;
    }

    public String getMaster() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, SPARK_PREFIX);
        String sparkMaster = resolver.getRequiredProperty("master");
        LOG.info("Resolve spark.master = [" + sparkMaster + "]");
        return sparkMaster;
    }

    public String getAppName() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, SPARK_PREFIX);
        String appName = resolver.getProperty("app.name");
        String finalAppName = appName == null ? "spring-default-app-name" : appName;
        LOG.info("Resolve spark.app.name = [" + finalAppName + "]");
        return finalAppName;
    }

    public Map<String, String> getOtherSparkConf() {
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(
                this.environment, SPARK_PREFIX);
        Map<String, Object> pro = resolver.getSubProperties(null);
        Map<String, String> fullProperities = new HashMap<String, String>();
        for (Map.Entry<String, Object> entry : pro.entrySet()) {
            String newKey = SPARK_PREFIX + entry.getKey();
            if(newKey.equals("spark.master") || newKey.equals("spark.app.name")){
                continue;
            }
            Object value = entry.getValue();
            String newValue = "";
            if (value instanceof String) {
                newValue = (String) value;
            } else if (value instanceof Class<?>) {
                newValue = ((Class) value).getCanonicalName();
            } else {
                newValue = value.toString();
            }
            fullProperities.put(newKey, newValue);
        }
        if(LOG.isInfoEnabled()) {
            if(fullProperities.isEmpty()){
                LOG.info("No spark properities resolved");
            }
            else {
                StringBuffer sb = new StringBuffer();
                sb.append("Resolve spark properities: [\n");
                for (Map.Entry<String, String> entry : fullProperities.entrySet()) {
                    sb.append("(" + entry.getKey() + " \t " + entry.getValue() + ")\n");
                }
                sb.append("]");
                LOG.info(sb.toString());
            }
        }
        return fullProperities;
    }
}
