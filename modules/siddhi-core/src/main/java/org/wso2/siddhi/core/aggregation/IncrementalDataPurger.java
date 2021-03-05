/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.aggregation;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.exception.DataPurgingException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.StoreQueryRuntime;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.IncrementalTimeConverterUtil;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.parser.StoreQueryParser;
import org.wso2.siddhi.core.window.Window;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.AggregationDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.execution.query.StoreQuery;
import org.wso2.siddhi.query.api.execution.query.input.store.InputStore;
import org.wso2.siddhi.query.api.execution.query.selection.OutputAttribute;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.wso2.siddhi.core.util.SiddhiConstants.AGG_EXTERNAL_TIMESTAMP_COL;
import static org.wso2.siddhi.core.util.SiddhiConstants.AGG_START_TIMESTAMP_COL;
import static org.wso2.siddhi.query.api.expression.Expression.Time.normalizeDuration;
import static org.wso2.siddhi.query.api.expression.Expression.Time.timeToLong;

/**
 * This class implements the logic which is needed to purge data which are related to incremental
 **/
public class IncrementalDataPurger implements Runnable {
    private static final Logger LOG = Logger.getLogger(IncrementalDataPurger.class);
    private long purgeExecutionInterval = Expression.Time.minute(15).value();
    private boolean purgingEnabled = true;
    private Map<TimePeriod.Duration, Long> retentionPeriods = new EnumMap<>(TimePeriod.Duration.class);
    private StreamEventPool streamEventPool;
    private Map<TimePeriod.Duration, Table> aggregationTables;
    private SiddhiAppContext siddhiAppContext;
    private ScheduledFuture scheduledPurgingTaskStatus;
    private String purgingTimestampField;
    private Map<TimePeriod.Duration, Long> minimumDurationMap = new EnumMap<>(TimePeriod.Duration.class);
    private static final Long RETAIN_ALL = -1L;
    private static final String RETAIN_ALL_VALUES = "all";
    private static final String AGGREGATION_START_TIME = "aggregationStartTime";
    private static final String AGGREGATION_NEXT_EMIT_TIME = "nextEmitTime";
    private static final String IS_DATA_AVAILABLE_TO_PURGE = "isDataAvailableToPurge";
    private static final String IS_PARENT_TABLE_HAS_AGGREGATED_DATA = "isParentTableHasAggregatedData";
    private ComplexEventChunk<StateEvent> eventChunk = new ComplexEventChunk<>(true);
    private List<VariableExpressionExecutor> variableExpressionExecutorList = new ArrayList<>();
    private Attribute aggregatedTimestampAttribute;
    private Map<TimePeriod.Duration, CompiledCondition> compiledConditionsHolder =
            new EnumMap<>(TimePeriod.Duration.class);
    private Map<String, Table> tableMap = new HashMap<>();
    private AggregationDefinition aggregationDefinition;
    private List<TimePeriod.Duration> activeIncrementalDurations;
    private String timeZone;
    private Map<String, Window> windowMap;
    private Map<String, AggregationRuntime> aggregationMap;
    private boolean purgingHalted = false;
    private String errorMessage;


    public void init(AggregationDefinition aggregationDefinition, StreamEventPool streamEventPool,
                     Map<TimePeriod.Duration, Table> aggregationTables, Boolean isProcessingOnExternalTime,
                     SiddhiAppContext siddhiAppContext, List<TimePeriod.Duration> incrementalDurations,
                     String timeZone, Map<String, Window> windowMap,
                     Map<String, AggregationRuntime> aggregationMap) {
        this.siddhiAppContext = siddhiAppContext;
        this.aggregationDefinition = aggregationDefinition;
        List<Annotation> annotations = aggregationDefinition.getAnnotations();
        this.streamEventPool = streamEventPool;
        this.aggregationTables = aggregationTables;
        this.activeIncrementalDurations = incrementalDurations;
        this.windowMap = windowMap;
        this.aggregationMap = aggregationMap;
        if (isProcessingOnExternalTime) {
            purgingTimestampField = AGG_EXTERNAL_TIMESTAMP_COL;
        } else {
            purgingTimestampField = AGG_START_TIMESTAMP_COL;
        }
        aggregatedTimestampAttribute = new Attribute(purgingTimestampField, Attribute.Type.LONG);

        VariableExpressionExecutor variableExpressionExecutor = new VariableExpressionExecutor(
                aggregatedTimestampAttribute, 0, 1);
        variableExpressionExecutorList.add(variableExpressionExecutor);
        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            this.tableMap.put(entry.getValue().getTableDefinition().getId(), entry.getValue());
            switch (entry.getKey()) {
                case SECONDS:
                    retentionPeriods.put(entry.getKey(), Expression.Time.sec(120).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.sec(120).value());
                    break;
                case MINUTES:
                    retentionPeriods.put(entry.getKey(), Expression.Time.hour(24).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.minute(120).value());
                    break;
                case HOURS:
                    retentionPeriods.put(entry.getKey(), Expression.Time.day(30).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.hour(25).value());
                    break;
                case DAYS:
                    retentionPeriods.put(entry.getKey(), Expression.Time.year(1).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.day(32).value());
                    break;
                case MONTHS:
                    retentionPeriods.put(entry.getKey(), RETAIN_ALL);
                    minimumDurationMap.put(entry.getKey(), Expression.Time.month(13).value());
                    break;
                case YEARS:
                    retentionPeriods.put(entry.getKey(), RETAIN_ALL);
                    minimumDurationMap.put(entry.getKey(), 0L);
            }
        }

        this.timeZone = timeZone;
        Map<String, Annotation> annotationTypes = new HashMap<>();
        for (Annotation annotation : annotations) {
            annotationTypes.put(annotation.getName().toLowerCase(), annotation);
        }
        Annotation purge = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE);
        if (purge != null) {
            if (purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE) != null) {
                String purgeEnable = purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE);
                if (!("true".equalsIgnoreCase(purgeEnable) || "false".equalsIgnoreCase(purgeEnable))) {
                    throw new SiddhiAppCreationException("Invalid value for enable: " + purgeEnable + "." +
                            " Please use true or false");
                } else {
                    purgingEnabled = Boolean.parseBoolean(purgeEnable);
                }
            }
            if (purgingEnabled) {
                // If interval is defined, default value of 15 min will be replaced by user input value
                if (purge.getElement(SiddhiConstants.NAMESPACE_INTERVAL) != null) {
                    String interval = purge.getElement(SiddhiConstants.NAMESPACE_INTERVAL);
                    purgeExecutionInterval = timeToLong(interval);
                }
                List<Annotation> retentions = purge.getAnnotations(SiddhiConstants.NAMESPACE_RETENTION);
                if (retentions != null && !retentions.isEmpty()) {
                    Annotation retention = retentions.get(0);
                    List<Element> elements = retention.getElements();
                    for (Element element : elements) {
                        TimePeriod.Duration duration = normalizeDuration(element.getKey());
                        if (!incrementalDurations.contains(duration)) {
                            throw new SiddhiAppCreationException(duration + " granularity cannot be purged since " +
                                    "aggregation has not performed in " + duration + " granularity");
                        }
                        if (element.getValue().equalsIgnoreCase(RETAIN_ALL_VALUES)) {
                            retentionPeriods.put(duration, RETAIN_ALL);
                        } else {
                            if (timeToLong(element.getValue()) >= minimumDurationMap.get(duration)) {
                                retentionPeriods.put(duration, timeToLong(element.getValue()));
                            } else {
                                throw new SiddhiAppCreationException(duration + " granularity cannot be purge" +
                                        " with a retention of '" + element.getValue() + "', minimum retention" +
                                        " should be greater  than " + TimeUnit.MILLISECONDS.toMinutes
                                        (minimumDurationMap.get(duration)) + " minutes");
                            }
                        }
                    }
                }
            }
        }
        compiledConditionsHolder = createCompileConditions(aggregationTables, tableMap);
    }

    public boolean isPurgingEnabled() {
        return purgingEnabled;
    }

    public void setPurgingEnabled(boolean purgingEnabled) {
        this.purgingEnabled = purgingEnabled;
    }

    @Override
    public void run() {
        boolean isNeededToExecutePurgeTask = false;
        Map<String, Boolean> purgingCheckState;
        boolean isSafeToRunPurgingTask = false;
        long currentTime = System.currentTimeMillis();
        long purgeTime;
        Object[] purgeTimeArray = new Object[1];
        int i = 1;
        if (purgingHalted) {
            LOG.error(errorMessage);
            return;
        }

        for (TimePeriod.Duration duration : activeIncrementalDurations) {
            if (!retentionPeriods.get(duration).equals(RETAIN_ALL)) {
                eventChunk.clear();
                purgeTime = currentTime - retentionPeriods.get(duration);
                purgeTimeArray[0] = purgeTime;

                if (retentionPeriods.size() > i) {
                    purgingCheckState = isSafeToPurgeTheDuration(purgeTime,
                            aggregationTables.get(activeIncrementalDurations.get(i)),
                            aggregationTables.get(duration), duration, timeZone);
                    if (purgingCheckState.get(IS_DATA_AVAILABLE_TO_PURGE)) {
                        isNeededToExecutePurgeTask = true;
                        if (purgingCheckState.get(IS_PARENT_TABLE_HAS_AGGREGATED_DATA)) {
                            isSafeToRunPurgingTask = true;
                        } else {
                            isSafeToRunPurgingTask = false;
                            purgingHalted = true;
                        }
                    } else {
                        isNeededToExecutePurgeTask = false;
                    }
                }
                if (isNeededToExecutePurgeTask) {
                    if (isSafeToRunPurgingTask) {
                        StateEvent secEvent = createStreamEvent(purgeTimeArray, currentTime);
                        eventChunk.add(secEvent);
                        Table table = aggregationTables.get(duration);
                        try {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Purging data of table: " + table.getTableDefinition().getId() + " with a" +
                                        " retention of timestamp : " + purgeTime);
                            }
                            table.deleteEvents(eventChunk, compiledConditionsHolder.get(duration), 1);
                        } catch (RuntimeException e) {
                            LOG.error("Exception occurred while deleting events from " +
                                    table.getTableDefinition().getId() + " table", e);
                            throw new DataPurgingException("Exception occurred while deleting events from " +
                                    table.getTableDefinition().getId() + " table", e);
                        }
                    } else {
                        errorMessage = "Purging task halted!!!. Data purging for table: "
                                + aggregationTables.get(duration).getTableDefinition().getId() + " with a retention"
                                + " of timestamp : " + purgeTime + " didn't executed since parent "
                                + aggregationTables.get(activeIncrementalDurations.get(i)).getTableDefinition().getId()
                                + " table does not contain values of above period. This has to be investigate since" +
                                " this may lead to an aggregation data mismatch";
                        LOG.info(errorMessage);
                        return;
                    }
                }
            }
            i++;
        }
    }

    /**
     * Building the MatchingMetaInfoHolder for delete records
     **/
    private MatchingMetaInfoHolder matchingMetaInfoHolder(Table table, Attribute attribute) {
        MetaStateEvent metaStateEvent = new MetaStateEvent(2);
        MetaStreamEvent metaStreamEventWithDeletePara = new MetaStreamEvent();
        MetaStreamEvent metaStreamEventForTable = new MetaStreamEvent();
        TableDefinition deleteTableDefinition = TableDefinition.id("");
        deleteTableDefinition.attribute(attribute.getName(), attribute.getType());
        metaStreamEventWithDeletePara.setEventType(MetaStreamEvent.EventType.TABLE);
        metaStreamEventWithDeletePara.addOutputData(attribute);
        metaStreamEventWithDeletePara.addInputDefinition(deleteTableDefinition);
        metaStreamEventForTable.setEventType(MetaStreamEvent.EventType.TABLE);
        for (Attribute attributes : table.getTableDefinition().getAttributeList()) {
            metaStreamEventForTable.addOutputData(attributes);
        }
        metaStreamEventForTable.addInputDefinition(table.getTableDefinition());
        metaStateEvent.addEvent(metaStreamEventWithDeletePara);
        metaStateEvent.addEvent(metaStreamEventForTable);
        TableDefinition definition = table.getTableDefinition();
        return new MatchingMetaInfoHolder(metaStateEvent, 0, 1,
                deleteTableDefinition, definition, 0);
    }

    /**
     * Building the compiled conditions for purge data
     **/
    private Map<TimePeriod.Duration, CompiledCondition> createCompileConditions(
            Map<TimePeriod.Duration, Table> aggregationTables, Map<String, Table> tableMap) {
        Map<TimePeriod.Duration, CompiledCondition> compiledConditionMap = new EnumMap<>(TimePeriod.Duration.class);
        CompiledCondition compiledCondition;
        Table table;
        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            if (!retentionPeriods.get(entry.getKey()).equals(RETAIN_ALL)) {
                table = aggregationTables.get(entry.getKey());
                Variable leftVariable = new Variable(purgingTimestampField);
                leftVariable.setStreamId(entry.getValue().getTableDefinition().getId());
                Compare expression = new Compare(leftVariable,
                        Compare.Operator.LESS_THAN, new Variable(purgingTimestampField));
                compiledCondition = table.compileCondition(expression,
                        matchingMetaInfoHolder(table, aggregatedTimestampAttribute), siddhiAppContext,
                        variableExpressionExecutorList, tableMap, table.getTableDefinition().getId() + "DeleteQuery");
                compiledConditionMap.put(entry.getKey(), compiledCondition);
            }
        }
        return compiledConditionMap;
    }

    /**
     * Data purging task scheduler method
     **/
    public void executeIncrementalDataPurging() {
        StringBuilder tableNames = new StringBuilder();
        if (isPurgingEnabled()) {
            if (scheduledPurgingTaskStatus != null) {
                scheduledPurgingTaskStatus.cancel(true);
                scheduledPurgingTaskStatus = siddhiAppContext.getScheduledExecutorService().
                        scheduleWithFixedDelay(this, purgeExecutionInterval, purgeExecutionInterval,
                                TimeUnit.MILLISECONDS);
            } else {
                scheduledPurgingTaskStatus = siddhiAppContext.getScheduledExecutorService().
                        scheduleWithFixedDelay(this, purgeExecutionInterval, purgeExecutionInterval,
                                TimeUnit.MILLISECONDS);
            }
            for (Map.Entry<TimePeriod.Duration, Long> entry : retentionPeriods.entrySet()) {
                if (!retentionPeriods.get(entry.getKey()).equals(RETAIN_ALL) &&
                        activeIncrementalDurations.contains(entry.getKey())) {
                    tableNames.append(entry.getKey()).append(",");
                }
            }
            LOG.info("Data purging has enabled for tables: " + tableNames + " with an interval of " +
                    ((purgeExecutionInterval) / 1000) + " seconds in " + aggregationDefinition.getId() +
                    " aggregation");
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Purging is disabled in siddhi app: " + siddhiAppContext.getName());
            }
        }
    }

    /**
     * creating stream event method
     **/
    private StateEvent createStreamEvent(Object[] values, Long timestamp) {
        StreamEvent streamEvent = streamEventPool.borrowEvent();
        streamEvent.setTimestamp(timestamp);
        streamEvent.setOutputData(values);
        StateEvent stateEvent = new StateEvent(2, 1);
        stateEvent.addEvent(0, streamEvent);
        return stateEvent;
    }

    private Map<String, Boolean> isSafeToPurgeTheDuration(long purgeTime, Table parentTable, Table currentTable,
                                                          TimePeriod.Duration duration, String timeZone) {
        Event[] dataToDelete;
        Event[] dataInParentTable = null;
        Map<String, Boolean> purgingCheckState = new HashMap<>();

        try {
            dataToDelete = dataToDelete(purgeTime, currentTable);

            if (dataToDelete != null && dataToDelete.length != 0) {
                Map<String, Long> purgingValidationTimeDurations = getPurgingValidationTimeDurations(duration,
                        (Long) dataToDelete[0].getData()[0], timeZone);
                StoreQuery storeQuery = getStoreQuery(parentTable, purgingValidationTimeDurations.
                        get(AGGREGATION_START_TIME), purgingValidationTimeDurations.get(AGGREGATION_NEXT_EMIT_TIME));
                storeQuery.setType(StoreQuery.StoreQueryType.FIND);
                StoreQueryRuntime storeQueryRuntime = StoreQueryParser.parse(storeQuery,
                        siddhiAppContext, tableMap, windowMap, aggregationMap);
                dataInParentTable = storeQueryRuntime.execute();
            }
            purgingCheckState.put(IS_DATA_AVAILABLE_TO_PURGE, dataToDelete != null && dataToDelete.length > 0);
            purgingCheckState.put(IS_PARENT_TABLE_HAS_AGGREGATED_DATA, dataInParentTable != null
                    && dataInParentTable.length > 0);
        } catch (Exception e) {
            LOG.error("Error occurred while checking whether the data is safe to purge from aggregation " +
                    "tables for the aggregation " + aggregationDefinition.getId(), e);
            purgingCheckState.put(IS_DATA_AVAILABLE_TO_PURGE, false);
            purgingCheckState.put(IS_PARENT_TABLE_HAS_AGGREGATED_DATA, false);
            errorMessage = "Error occurred while checking whether the data is safe to purge from aggregation tables" +
                    " for the aggregation " + aggregationDefinition.getId();
            purgingHalted = true;
        }
        return purgingCheckState;
    }

    private StoreQuery getStoreQuery(Table table, long timeFrom, long timeTo) {
        List<OutputAttribute> outputAttributes = new ArrayList<>();
        outputAttributes.add(new OutputAttribute(new Variable(AGG_START_TIMESTAMP_COL)));
        Selector selector = Selector.selector().addSelectionList(outputAttributes)
                .groupBy(Expression.variable(AGG_START_TIMESTAMP_COL))
                .limit(Expression.value(1));
        InputStore inputStore;
        if (timeTo != 0) {
            inputStore = InputStore.store(table.getTableDefinition().getId()).
                    on(Expression.and(
                            Expression.compare(
                                    Expression.variable(AGG_START_TIMESTAMP_COL),
                                    Compare.Operator.GREATER_THAN_EQUAL,
                                    Expression.value(timeFrom)
                            ),
                            Expression.compare(
                                    Expression.variable(AGG_START_TIMESTAMP_COL),
                                    Compare.Operator.LESS_THAN_EQUAL,
                                    Expression.value(timeTo)
                            )
                    ));
        } else {
            inputStore = InputStore.store(table.getTableDefinition().getId()).on(Expression.compare(
                    Expression.variable(AGG_START_TIMESTAMP_COL),
                    Compare.Operator.LESS_THAN_EQUAL,
                    Expression.value(timeFrom)
            ));
        }

        return StoreQuery.query().from(inputStore).select(selector);
    }

    private Map<String, Long> getPurgingValidationTimeDurations(TimePeriod.Duration duration, long purgeTime,
                                                                String timeZone) {
        long aggregtionStartTime;
        long nextEmmitTime;
        Map<String, Long> purgingValidationTimeDuration = new HashMap<>();
        switch (duration) {
            case SECONDS:
                aggregtionStartTime = IncrementalTimeConverterUtil.
                        getStartTimeOfAggregates(purgeTime, TimePeriod.Duration.MINUTES, timeZone);
                nextEmmitTime = IncrementalTimeConverterUtil.getNextEmitTime(purgeTime, TimePeriod.Duration.MINUTES,
                        timeZone);
                purgingValidationTimeDuration.put(AGGREGATION_START_TIME, aggregtionStartTime);
                purgingValidationTimeDuration.put(AGGREGATION_NEXT_EMIT_TIME, nextEmmitTime);
                return purgingValidationTimeDuration;
            case MINUTES:
                aggregtionStartTime = IncrementalTimeConverterUtil.
                        getStartTimeOfAggregates(purgeTime, TimePeriod.Duration.HOURS, timeZone);
                nextEmmitTime = IncrementalTimeConverterUtil.getNextEmitTime(purgeTime, TimePeriod.Duration.HOURS,
                        timeZone);
                purgingValidationTimeDuration.put(AGGREGATION_START_TIME, aggregtionStartTime);
                purgingValidationTimeDuration.put(AGGREGATION_NEXT_EMIT_TIME, nextEmmitTime);
                return purgingValidationTimeDuration;
            case HOURS:
                aggregtionStartTime = IncrementalTimeConverterUtil.
                        getStartTimeOfAggregates(purgeTime, TimePeriod.Duration.DAYS, timeZone);
                nextEmmitTime = IncrementalTimeConverterUtil.getNextEmitTime(purgeTime, TimePeriod.Duration.DAYS,
                        timeZone);
                purgingValidationTimeDuration.put(AGGREGATION_START_TIME, aggregtionStartTime);
                purgingValidationTimeDuration.put(AGGREGATION_NEXT_EMIT_TIME, nextEmmitTime);
                return purgingValidationTimeDuration;
            case DAYS:
                aggregtionStartTime = IncrementalTimeConverterUtil.
                        getStartTimeOfAggregates(purgeTime, TimePeriod.Duration.MONTHS, timeZone);
                nextEmmitTime = IncrementalTimeConverterUtil.getNextEmitTime(purgeTime, TimePeriod.Duration.MONTHS,
                        timeZone);
                purgingValidationTimeDuration.put(AGGREGATION_START_TIME, aggregtionStartTime);
                purgingValidationTimeDuration.put(AGGREGATION_NEXT_EMIT_TIME, nextEmmitTime);
                return purgingValidationTimeDuration;
            case MONTHS:
                aggregtionStartTime = IncrementalTimeConverterUtil.
                        getStartTimeOfAggregates(purgeTime, TimePeriod.Duration.YEARS, timeZone);
                nextEmmitTime = IncrementalTimeConverterUtil.getNextEmitTime(purgeTime, TimePeriod.Duration.YEARS,
                        timeZone);
                purgingValidationTimeDuration.put(AGGREGATION_START_TIME, aggregtionStartTime);
                purgingValidationTimeDuration.put(AGGREGATION_NEXT_EMIT_TIME, nextEmmitTime);
                return purgingValidationTimeDuration;
        }
        return purgingValidationTimeDuration;
    }

    Event[] dataToDelete(long purgingTime, Table table) {
        StoreQuery storeQuery = getStoreQuery(table, purgingTime, 0);
        storeQuery.setType(StoreQuery.StoreQueryType.FIND);
        StoreQueryRuntime storeQueryRuntime = StoreQueryParser.parse(storeQuery,
                siddhiAppContext, tableMap, windowMap, aggregationMap);
        return storeQueryRuntime.execute();
    }

}

