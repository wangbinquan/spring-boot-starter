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

package org.wbq.spring.boot.autoconfigure.spark.proxy;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SparkContextProxy extends SparkContext {
    private SparkContextStopCallback sparkContextStopCallback = null;
    public SparkContextProxy(SparkConf sparkConf, SparkContextStopCallback sparkContextStopCallback){
        super(sparkConf);
        this.sparkContextStopCallback = sparkContextStopCallback;
    }
    @Override
    public void stop(){
        if (sparkContextStopCallback != null && sparkContextStopCallback.onSparkContextStop()){
            try {
                super.stop();
            }
            catch (Throwable e){

            }
            finally {
                sparkContextStopCallback.afterSparkContextStop();
            }
        }
    }
}
