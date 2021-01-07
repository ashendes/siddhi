/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.core.aggregation.persistedaggregation.config;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;

import java.util.Date;

/**
 * Processor implementation to read the results after executing the aggregation query
 **/
public class PersistedAggregationResultsProcessor implements Processor {
    private static final Logger log = Logger.getLogger(PersistedAggregationResultsProcessor.class);
    private TimePeriod.Duration duration;

    public PersistedAggregationResultsProcessor(TimePeriod.Duration duration) {
        this.duration = duration;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        if (complexEventChunk != null) {
            ComplexEvent complexEvent = complexEventChunk.getFirst();
            Object[] outputData = complexEvent.getOutputData();
            if (outputData.length == 3) {
                Date fromTime = new Date((Long) outputData[0]);
                Date toTime = new Date((Long) outputData[1]);
                log.info("Aggregation executed for duration " + duration + " from " + fromTime + " to " +
                        toTime + " and  " + outputData[2] + " records has been successfully updated ");
            }
        }
    }

    @Override
    public Processor getNextProcessor() {
        return null;
    }

    @Override
    public void setNextProcessor(Processor processor) {

    }

    @Override
    public void setToLast(Processor processor) {

    }

    @Override
    public Processor cloneProcessor(String key) {

        return null;
    }

    @Override
    public void clean() {

    }
}
