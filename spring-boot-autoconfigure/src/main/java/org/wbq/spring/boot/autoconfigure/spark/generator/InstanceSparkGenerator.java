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

package org.wbq.spring.boot.autoconfigure.spark.generator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.wbq.spring.boot.autoconfigure.properties.SparkProperties;
import org.wbq.spring.boot.autoconfigure.spark.proxy.SparkContextProxy;
import org.wbq.spring.boot.autoconfigure.spark.proxy.SparkContextStopCallback;

import java.util.Map;

public class InstanceSparkGenerator implements SparkGenerator {
    private SparkSession sparkSession = null;
    private final SparkConf sparkConf = new SparkConf();
    private final Object lock = new Object();
    private final SparkContextStopCallback sparkContextStopCallback = new SparkContextStopCallback() {
        @Override
        public boolean onSparkContextStop(){
            return false;
        }

        @Override
        public void afterSparkContextStop() {

        }
    };
    public InstanceSparkGenerator(SparkProperties sparkProperties){
        sparkConf.setMaster(sparkProperties.getMaster())
                .setAppName(sparkProperties.getAppName());
        for (Map.Entry<String, String> entry : sparkProperties.getOtherSparkConf().entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public SparkSession getSparkSession() {
        if (sparkSession == null){
            synchronized (lock) {
                if (sparkSession == null){
                    SparkContext sparkContext = new SparkContextProxy(sparkConf, this.sparkContextStopCallback);
                    this.sparkSession = SparkSession.builder().sparkContext(sparkContext)
                            .config(sparkConf)
                            .getOrCreate();
                    return sparkSession;
                }
                else {
                    return sparkSession;
                }
            }
        }
        else {
            return sparkSession;
        }
    }

}
