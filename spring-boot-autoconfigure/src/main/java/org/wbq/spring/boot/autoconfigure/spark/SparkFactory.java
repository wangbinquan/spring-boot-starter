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

package org.wbq.spring.boot.autoconfigure.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.wbq.spring.boot.autoconfigure.properties.SparkProperties;
import org.wbq.spring.boot.autoconfigure.spark.generator.InstanceSparkGenerator;
import org.wbq.spring.boot.autoconfigure.spark.generator.ShareSparkGenerator;
import org.wbq.spring.boot.autoconfigure.spark.generator.SparkGenerator;

public class SparkFactory {
    private SparkGenerator sparkGenerator = null;
    public SparkFactory(SparkProperties sparkProperties) {
        String sparkGeneratorType = sparkProperties.getSparkGeneratorType();
        if (sparkGeneratorType != null && sparkGeneratorType.equals("instance")){
            this.sparkGenerator = new InstanceSparkGenerator(sparkProperties);
        }
        else {
            this.sparkGenerator = new ShareSparkGenerator(sparkProperties);
        }
    }

    public SparkSession getSparkSession() {
        return this.sparkGenerator.getSparkSession();
    }

    public SparkContext getSparkContext() {
        return this.sparkGenerator.getSparkSession().sparkContext();
    }
}
