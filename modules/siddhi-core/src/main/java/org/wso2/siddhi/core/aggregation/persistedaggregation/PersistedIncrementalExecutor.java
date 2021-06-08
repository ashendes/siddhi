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

package org.wso2.siddhi.core.aggregation.persistedaggregation;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.aggregation.Executor;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventFactory;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.util.IncrementalTimeConverterUtil;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Incremental Executor implementation class for Persisted Aggregation
 **/
public class PersistedIncrementalExecutor implements Executor, Snapshotable {
    private static final Logger log = Logger.getLogger(PersistedIncrementalExecutor.class);

    private final ExpressionExecutor timestampExpressionExecutor;
    private final String aggregatorName;
    private TimePeriod.Duration duration;
    private Executor next;
    private StreamEventFactory streamEventFactory;
    private String timeZone;
    private Processor cudStreamProcessor;
    private boolean isProcessingExecutor;
    private long nextEmitTime = -1;
    private long startTimeOfAggregates = -1;
    private StreamEventPool streamEventPool;
    private boolean timerStarted = false;
    private String elementId;
    private SiddhiAppContext siddhiAppContext;
    private LinkedBlockingQueue<QueuedCudStreamProcessor> cudStreamProcessorQueue;

    public PersistedIncrementalExecutor(String aggregatorName, TimePeriod.Duration duration,
                                        List<ExpressionExecutor> processExpressionExecutors,
                                        Executor child, MetaStreamEvent metaStreamEvent, String timeZone,
                                        Processor cudStreamProcessor, SiddhiAppContext siddhiAppContext,
                                        LinkedBlockingQueue<QueuedCudStreamProcessor> cudStreamProcessorQueue) {
        this.timeZone = timeZone;
        this.duration = duration;
        this.next = child;
        this.cudStreamProcessor = cudStreamProcessor;
        this.aggregatorName = aggregatorName;
        this.siddhiAppContext = siddhiAppContext;

        this.timestampExpressionExecutor = processExpressionExecutors.remove(0);
        this.streamEventFactory = new StreamEventFactory(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        this.streamEventPool = new StreamEventPool(metaStreamEvent, 10);
        setNextExecutor(child);
        this.isProcessingExecutor = false;
        this.cudStreamProcessorQueue = cudStreamProcessorQueue;
        if (aggregatorName != null) {
            elementId = "IncrementalExecutor-" + siddhiAppContext.getElementIdGenerator().createNewId();
            siddhiAppContext.getSnapshotService().addSnapshotable(aggregatorName, this);
        }
    }

    @Override
    public void execute(ComplexEventChunk streamEventChunk) {
        if (log.isDebugEnabled()) {
            log.debug("Event Chunk received by " + this.duration + " incremental executor: " +
                    streamEventChunk.toString() + " will be dropped since persisted aggregation has been scheduled ");
        }
        streamEventChunk.reset();
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = (StreamEvent) streamEventChunk.next();
            streamEventChunk.remove();

            long timestamp = getTimestamp(streamEvent);
            long startedTime = startTimeOfAggregates;
            startTimeOfAggregates = IncrementalTimeConverterUtil.getStartTimeOfAggregates(timestamp, duration,
                    timeZone);
            if (timestamp >= nextEmitTime) {
                long emittedTime = nextEmitTime;
                nextEmitTime = IncrementalTimeConverterUtil.getNextEmitTime(timestamp, duration, timeZone);
                dispatchAggregateEvents(startedTime, emittedTime, timeZone);
                sendTimerEvent();
            }
        }
    }

    private void dispatchAggregateEvents(long startTimeOfNewAggregates, long emittedTime, String timeZone) {
        if (emittedTime != -1) {
            dispatchEvent(startTimeOfNewAggregates, emittedTime, timeZone);
        }
    }

    private void dispatchEvent(long startTimeOfNewAggregates, long emittedTime, String timeZone) {
        ZonedDateTime startTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTimeOfNewAggregates),
                ZoneId.of(timeZone));
        ZonedDateTime endTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(emittedTime),
                ZoneId.of(timeZone));
        log.info("Aggregation event dispatched for the duration " + duration + " to aggregate data from "
                + startTime.toString() + " to " + endTime.toString() + " ");
        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
        StreamEvent streamEvent = streamEventFactory.newInstance();
        streamEvent.setType(ComplexEvent.Type.CURRENT);
        streamEvent.setTimestamp(emittedTime);
        List<Object> outputDataList = new ArrayList<>();
        outputDataList.add(startTimeOfNewAggregates);
        outputDataList.add(emittedTime);
        outputDataList.add(null);
        streamEvent.setOutputData(outputDataList.toArray());

        if (isProcessingExecutor) {
            complexEventChunk.add(streamEvent);
            cudStreamProcessorQueue.add(new QueuedCudStreamProcessor(cudStreamProcessor, streamEvent,
                    startTimeOfNewAggregates, emittedTime, timeZone, duration));
        }
        if (getNextExecutor() != null) {
            next.execute(complexEventChunk);
        }
    }

    public void setEmitTimestamp(long emitTimeOfLatestEventInTable) {
        this.nextEmitTime = emitTimeOfLatestEventInTable;
    }

    public void setProcessingExecutor(boolean processingExecutor) {
        isProcessingExecutor = processingExecutor;
    }

    private void sendTimerEvent() {
        if (getNextExecutor() != null) {
            StreamEvent timerEvent = streamEventPool.borrowEvent();
            timerEvent.setType(ComplexEvent.Type.TIMER);
            timerEvent.setTimestamp(startTimeOfAggregates);
            ComplexEventChunk<StreamEvent> timerStreamEventChunk = new ComplexEventChunk<>(true);
            timerStreamEventChunk.add(timerEvent);
            next.execute(timerStreamEventChunk);
        }
    }

    private long getTimestamp(StreamEvent streamEvent) {
        long timestamp;
        if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
            timestamp = (long) timestampExpressionExecutor.execute(streamEvent);
        } else {
            timestamp = streamEvent.getTimestamp();
        }
        return timestamp;
    }

    @Override
    public Executor getNextExecutor() {
        return next;
    }

    @Override
    public void setNextExecutor(Executor executor) {
        next = executor;
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();

        state.put("NextEmitTime", nextEmitTime);
        state.put("StartTimeOfAggregates", startTimeOfAggregates);
        state.put("TimerStarted", timerStarted);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        nextEmitTime = (long) state.get("NextEmitTime");
        startTimeOfAggregates = (long) state.get("StartTimeOfAggregates");
        timerStarted = (boolean) state.get("TimerStarted");
    }

    @Override
    public String getElementId() {
        return elementId;
    }

    @Override
    public void clean() {
        timestampExpressionExecutor.clean();
        siddhiAppContext.getSnapshotService().removeSnapshotable(aggregatorName, this);
    }
}
