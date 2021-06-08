/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.util.parser;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.aggregation.AggregationRuntime;
import org.wso2.siddhi.core.aggregation.Executor;
import org.wso2.siddhi.core.aggregation.IncrementalAggregationProcessor;
import org.wso2.siddhi.core.aggregation.IncrementalDataPurger;
import org.wso2.siddhi.core.aggregation.IncrementalExecutor;
import org.wso2.siddhi.core.aggregation.IncrementalExecutorsInitialiser;
import org.wso2.siddhi.core.aggregation.persistedaggregation.PersistedIncrementalExecutor;
import org.wso2.siddhi.core.aggregation.persistedaggregation.config.DBAggregationQueryConfigurationEntry;
import org.wso2.siddhi.core.aggregation.persistedaggregation.config.DBAggregationQueryUtil;
import org.wso2.siddhi.core.aggregation.persistedaggregation.config.DBAggregationSelectFunctionTemplate;
import org.wso2.siddhi.core.aggregation.persistedaggregation.config.DBAggregationSelectQueryTemplate;
import org.wso2.siddhi.core.aggregation.persistedaggregation.config.DBAggregationTimeConversionDurationMapping;
import org.wso2.siddhi.core.aggregation.persistedaggregation.config.PersistedAggregationResultsProcessor;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.exception.CannotLoadConfigurationException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.incremental.IncrementalAggregateBaseTimeFunctionExecutor;
import org.wso2.siddhi.core.query.input.stream.StreamRuntime;
import org.wso2.siddhi.core.query.input.stream.single.EntryValveExecutor;
import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.AbstractStreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.QueryableProcessor;
import org.wso2.siddhi.core.query.selector.GroupByKeyGenerator;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.MaxAttributeAggregator;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.MinAttributeAggregator;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.SumAttributeAggregator;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.incremental.IncrementalAttributeAggregator;
import org.wso2.siddhi.core.query.selector.attribute.processor.executor.GroupByAggregationAttributeExecutor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.ExceptionUtil;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.SiddhiAppRuntimeBuilder;
import org.wso2.siddhi.core.util.SiddhiClassLoader;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.config.ConfigManager;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.extension.holder.FunctionExecutorExtensionHolder;
import org.wso2.siddhi.core.util.extension.holder.IncrementalAttributeAggregatorExtensionHolder;
import org.wso2.siddhi.core.util.extension.holder.StreamProcessorExtensionHolder;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.parser.helper.QueryParserHelper;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;
import org.wso2.siddhi.core.util.statistics.ThroughputTracker;
import org.wso2.siddhi.core.window.Window;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.AggregationDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamFunction;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamHandler;
import org.wso2.siddhi.query.api.execution.query.selection.OutputAttribute;
import org.wso2.siddhi.query.api.expression.AttributeFunction;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.Extension;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.wso2.siddhi.core.util.ExpressionExecutorClonerUtil.getExpressionExecutorClone;
import static org.wso2.siddhi.core.util.ExpressionExecutorClonerUtil.getExpressionExecutorClones;
import static org.wso2.siddhi.core.util.SiddhiConstants.AGG_EXTERNAL_TIMESTAMP_COL;
import static org.wso2.siddhi.core.util.SiddhiConstants.AGG_LAST_TIMESTAMP_COL;
import static org.wso2.siddhi.core.util.SiddhiConstants.AGG_SHARD_ID_COL;
import static org.wso2.siddhi.core.util.SiddhiConstants.AGG_START_TIMESTAMP_COL;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_BUFFER_SIZE;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_ELEMENT_ENABLE;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_IGNORE_EVENTS_OLDER_THAN_BUFFER;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PARTITION_BY_ID;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PERSISTED_AGGREGATION;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PRIMARY_KEY;
import static org.wso2.siddhi.core.util.SiddhiConstants.EQUALS;
import static org.wso2.siddhi.core.util.SiddhiConstants.FROM_TIMESTAMP;
import static org.wso2.siddhi.core.util.SiddhiConstants.FUNCTION_NAME_CUD;
import static org.wso2.siddhi.core.util.SiddhiConstants.INNER_SELECT_QUERY_REF_T3;
import static org.wso2.siddhi.core.util.SiddhiConstants.METRIC_INFIX_AGGREGATIONS;
import static org.wso2.siddhi.core.util.SiddhiConstants.METRIC_TYPE_FIND;
import static org.wso2.siddhi.core.util.SiddhiConstants.METRIC_TYPE_INSERT;
import static org.wso2.siddhi.core.util.SiddhiConstants.NAMESPACE_RDBMS;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_COLUMN;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_COLUMNS;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_CONDITION;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_DURATION;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_FROM_CONDITION;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_INNER_QUERY_1;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_INNER_QUERY_2;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_ON_CONDITION;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_SELECTORS;
import static org.wso2.siddhi.core.util.SiddhiConstants.PLACEHOLDER_TABLE_NAME;
import static org.wso2.siddhi.core.util.SiddhiConstants.SQL_AND;
import static org.wso2.siddhi.core.util.SiddhiConstants.SQL_AS;
import static org.wso2.siddhi.core.util.SiddhiConstants.SQL_FROM;
import static org.wso2.siddhi.core.util.SiddhiConstants.SQL_NOT_NULL;
import static org.wso2.siddhi.core.util.SiddhiConstants.SQL_SELECT;
import static org.wso2.siddhi.core.util.SiddhiConstants.SQL_WHERE;
import static org.wso2.siddhi.core.util.SiddhiConstants.SUB_SELECT_QUERY_REF_T1;
import static org.wso2.siddhi.core.util.SiddhiConstants.SUB_SELECT_QUERY_REF_T2;
import static org.wso2.siddhi.core.util.SiddhiConstants.TO_TIMESTAMP;

/**
 * This is the parser class of incremental aggregation definition.
 */
public class AggregationParser {

    private static final Logger log = Logger.getLogger(AggregationParser.class);

    public static AggregationRuntime parse(AggregationDefinition aggregationDefinition,
                                           SiddhiAppContext siddhiAppContext,
                                           Map<String, AbstractDefinition> streamDefinitionMap,
                                           Map<String, AbstractDefinition> tableDefinitionMap,
                                           Map<String, AbstractDefinition> windowDefinitionMap,
                                           Map<String, AbstractDefinition> aggregationDefinitionMap,
                                           Map<String, Table> tableMap,
                                           Map<String, Window> windowMap,
                                           Map<String, AggregationRuntime> aggregationMap,
                                           SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder) {
        String timeZone = getTimeZone(siddhiAppContext);
        boolean isDebugEnabled = log.isDebugEnabled();
        boolean isPersistedAggregation = false;
        if (!validateTimeZone(timeZone)) {
            throw new SiddhiAppCreationException(
                    "Given timeZone '" + timeZone + "' for aggregations is invalid. Please provide a valid time zone."
            );
        }
        //get aggregation name
        String aggregatorName = aggregationDefinition.getId();

        if (isDebugEnabled) {
            log.debug("Incremental aggregation initialization process started for aggregation "
                    + aggregatorName);
        }
        Annotation aggregationProperties = AnnotationHelper.getAnnotation(ANNOTATION_PERSISTED_AGGREGATION,
                aggregationDefinition.getAnnotations());
        if (aggregationProperties != null) {
            String persistedAggregationMode = aggregationProperties.getElement(ANNOTATION_ELEMENT_ENABLE);
            isPersistedAggregation = persistedAggregationMode == null || Boolean.parseBoolean(persistedAggregationMode);
        }

        if (isPersistedAggregation) {
            aggregationDefinition.getSelector().getSelectionList().stream().forEach(outputAttribute -> {
                if (outputAttribute.getExpression() instanceof AttributeFunction &&
                        ((AttributeFunction) outputAttribute.getExpression()).getName().equals("distinctCount")) {
                    throw new SiddhiAppCreationException("Aggregation function 'distinctCount' does not supported " +
                            "with persisted aggregation type please use default incremental aggregation");
                }
            });
        }
        if (isDebugEnabled) {
            log.debug("Aggregation mode is defined as " + (isPersistedAggregation ? "persisted" : "inMemory") +
                    " for aggregation " + aggregatorName);
        }
        if (aggregationDefinition.getTimePeriod() == null) {
            throw new SiddhiAppCreationException(
                    "AggregationDefinition '" + aggregationDefinition.getId() + "'s timePeriod is null. " +
                            "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'",
                    aggregationDefinition.getQueryContextStartIndex(), aggregationDefinition.getQueryContextEndIndex());
        }
        if (aggregationDefinition.getSelector() == null) {
            throw new SiddhiAppCreationException(
                    "AggregationDefinition '" + aggregationDefinition.getId() + "'s selection is not defined. " +
                            "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'",
                    aggregationDefinition.getQueryContextStartIndex(), aggregationDefinition.getQueryContextEndIndex());
        }
        if (streamDefinitionMap.get(aggregationDefinition.getBasicSingleInputStream().getStreamId()) == null) {
            throw new SiddhiAppCreationException("Stream " + aggregationDefinition.getBasicSingleInputStream().
                    getStreamId() + " has not been defined");
        }

        //Check if the user defined primary keys exists for @store annotation on aggregation if yes, an error is
        //is thrown
        Element userDefinedPrimaryKey = AnnotationHelper.getAnnotationElement(ANNOTATION_PRIMARY_KEY,
                null, aggregationDefinition.getAnnotations());
        if (userDefinedPrimaryKey != null) {
            throw new SiddhiAppCreationException("Aggregation Tables have predefined primary key, but found '" +
                    userDefinedPrimaryKey.getValue() + "' primary key defined though annotation.");
        }

        Element element = AnnotationHelper.getAnnotationElement(ANNOTATION_BUFFER_SIZE, null,
                aggregationDefinition.getAnnotations());
        if (element != null) {
            log.info("@BufferSize annotation is depreciated. Out of order events are handled without buffers.");
        }

        element = AnnotationHelper.getAnnotationElement(ANNOTATION_IGNORE_EVENTS_OLDER_THAN_BUFFER,
                null, aggregationDefinition.getAnnotations());
        if (element != null) {
            log.info("@IgnoreEventsOlderThanBuffer annotation is depreciated. Out of order events are handled " +
                    "without buffers.");
        }

        try {
            List<VariableExpressionExecutor> incomingVariableExpressionExecutors = new ArrayList<>();


            StreamRuntime streamRuntime = InputStreamParser.parse(aggregationDefinition.getBasicSingleInputStream(),
                    siddhiAppContext, null, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap,
                    aggregationDefinitionMap, tableMap, windowMap, aggregationMap, incomingVariableExpressionExecutors,
                    null, false, aggregatorName);

            // Get original meta for later use.
            MetaStreamEvent incomingMetaStreamEvent = (MetaStreamEvent) streamRuntime.getMetaComplexEvent();
            // Create new meta stream event.
            // This must hold the timestamp, group by attributes (if given) and the incremental attributes, in
            // onAfterWindowData array
            // Example format: AGG_TIMESTAMP, groupByAttribute1, groupByAttribute2, AGG_incAttribute1, AGG_incAttribute2
            // AGG_incAttribute1, AGG_incAttribute2 would have the same attribute names as in
            // finalListOfIncrementalAttributes
            incomingMetaStreamEvent.initializeAfterWindowData(); // To enter data as onAfterWindowData


            //List of all aggregationDurations
            List<TimePeriod.Duration> aggregationDurations =
                    getSortedPeriods(aggregationDefinition.getTimePeriod(), isPersistedAggregation);

            //Incoming executors will be executors for timestamp, externalTimestamp(if used),
            List<ExpressionExecutor> incomingExpressionExecutors = new ArrayList<>();
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators = new ArrayList<>();
            // group by attributes (if given) and the incremental attributes expression executors
            List<Variable> groupByVariableList = aggregationDefinition.getSelector().getGroupByList();

            //Expressions to get final aggregate outputs. e.g for avg the expression is Divide expression with
            // AGG_SUM/ AGG_COUNT
            List<Expression> outputExpressions = new ArrayList<>();

            boolean isProcessingOnExternalTime = aggregationDefinition.getAggregateAttribute() != null;
            boolean isGroupBy = aggregationDefinition.getSelector().getGroupByList().size() != 0;

            final boolean isDistributed;
            ConfigManager configManager = siddhiAppContext.getSiddhiContext().getConfigManager();
            final String shardId = configManager.extractProperty("shardId");
            boolean enablePartitioning = false;
            // check if the setup is Active Active(distributed deployment) by checking availability of partitionById
            // config
            Annotation partitionById = AnnotationHelper.getAnnotation(ANNOTATION_PARTITION_BY_ID,
                    aggregationDefinition.getAnnotations());
            if (partitionById != null) {
                String enableElement = partitionById.getElement(ANNOTATION_ELEMENT_ENABLE);
                enablePartitioning = enableElement == null || Boolean.parseBoolean(enableElement);
            }
            boolean shouldPartitionById = Boolean.parseBoolean(configManager.extractProperty("partitionById"));
            if (enablePartitioning || shouldPartitionById) {
                if (shardId == null) {
                    throw new SiddhiAppCreationException("Configuration 'shardId' not provided for @partitionbyid " +
                            "annotation");
                }
                isDistributed = true;
            } else {
                isDistributed = false;
            }

            if (isDebugEnabled) {
                log.debug("Distributed aggregation processing is " + (isDistributed ? "enabled" : "disabled") +
                        " in " + aggregatorName + " aggregation ");
            }

            populateIncomingAggregatorsAndExecutors(aggregationDefinition, siddhiAppContext, tableMap,
                    incomingVariableExpressionExecutors, aggregatorName, incomingMetaStreamEvent,
                    incomingExpressionExecutors, incrementalAttributeAggregators, groupByVariableList,
                    outputExpressions, isProcessingOnExternalTime, isDistributed, shardId);

            // check if the populateIncomingAggregatorsAndExecutors process has been completed successfully
            boolean isLatestEventColAdded = incomingMetaStreamEvent.getOutputData()
                    .get(incomingMetaStreamEvent.getOutputData().size() - 1)
                    .getName().equals(AGG_LAST_TIMESTAMP_COL);
            int baseAggregatorBeginIndex = incomingMetaStreamEvent.getOutputData().size();

            List<Expression> finalBaseExpressions = new ArrayList<>();
            processFinalBaseAggregators(siddhiAppContext, tableMap, incomingVariableExpressionExecutors, aggregatorName,
                    incomingMetaStreamEvent, incomingExpressionExecutors, incrementalAttributeAggregators,
                    finalBaseExpressions);

            // Creating an intermediate stream with aggregated stream and above extracted output variables
            StreamDefinition incomingOutputStreamDefinition = StreamDefinition.id(aggregatorName + "_intermediate");
            incomingOutputStreamDefinition.setQueryContextStartIndex(aggregationDefinition.getQueryContextStartIndex());
            incomingOutputStreamDefinition.setQueryContextEndIndex(aggregationDefinition.getQueryContextEndIndex());
            MetaStreamEvent processedMetaStreamEvent = new MetaStreamEvent();
            for (Attribute attribute : incomingMetaStreamEvent.getOutputData()) {
                incomingOutputStreamDefinition.attribute(attribute.getName(), attribute.getType());
                processedMetaStreamEvent.addOutputData(attribute);
            }
            incomingMetaStreamEvent.setOutputDefinition(incomingOutputStreamDefinition);
            processedMetaStreamEvent.addInputDefinition(incomingOutputStreamDefinition);
            processedMetaStreamEvent.setOutputDefinition(incomingOutputStreamDefinition);

            // Executors of processing meta
            List<VariableExpressionExecutor> processVariableExpressionExecutors = new ArrayList<>();

            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap = new HashMap<>();
            aggregationDurations.forEach(
                    incrementalDuration ->
                            processExpressionExecutorsMap.put(
                                    incrementalDuration,
                                    constructProcessExpressionExecutors(
                                            siddhiAppContext, tableMap, aggregatorName, baseAggregatorBeginIndex,
                                            finalBaseExpressions, incomingOutputStreamDefinition, processedMetaStreamEvent,
                                            processVariableExpressionExecutors, isGroupBy, isProcessingOnExternalTime,
                                            incrementalDuration, isDistributed, shardId, isLatestEventColAdded)));


            ExpressionExecutor shouldUpdateTimestamp = null;
            if (isLatestEventColAdded) {
                Expression shouldUpdateExp = AttributeFunction.function("incrementalAggregator", "shouldUpdate",
                        new Variable(AGG_LAST_TIMESTAMP_COL));
                shouldUpdateTimestamp = ExpressionParser.parseExpression(shouldUpdateExp, processedMetaStreamEvent,
                        0, tableMap, processVariableExpressionExecutors, siddhiAppContext, false, 0, aggregatorName);
            }

            //Expression executors to get
            List<ExpressionExecutor> outputExpressionExecutors = outputExpressions.stream()
                    .map(expression -> ExpressionParser.parseExpression(expression, processedMetaStreamEvent, 0, tableMap,
                            processVariableExpressionExecutors, siddhiAppContext, isGroupBy, 0, aggregatorName))
                    .collect(Collectors.toList());

            // Create group by key generator
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMap = new HashMap<>();
            aggregationDurations.forEach(
                    incrementalDuration -> {
                        GroupByKeyGenerator groupByKeyGenerator = null;
                        if (isProcessingOnExternalTime || isGroupBy) {
                            List<Expression> groupByExpressionList = new ArrayList<>();
                            if (isProcessingOnExternalTime) {
                                Expression externalTimestampExpression =
                                        AttributeFunction.function(
                                                "incrementalAggregator", "getAggregationStartTime",
                                                new Variable(AGG_EXTERNAL_TIMESTAMP_COL),
                                                new StringConstant(incrementalDuration.name())
                                        );
                                groupByExpressionList.add(externalTimestampExpression);
                            }
                            groupByExpressionList.addAll(groupByVariableList.stream()
                                    .map(groupByVariable -> (Expression) groupByVariable)
                                    .collect(Collectors.toList()));
                            groupByKeyGenerator = new GroupByKeyGenerator(groupByExpressionList,
                                    processedMetaStreamEvent,
                                    SiddhiConstants.UNKNOWN_STATE, tableMap, processVariableExpressionExecutors,
                                    siddhiAppContext, aggregatorName);
                        }
                        groupByKeyGeneratorMap.put(incrementalDuration, groupByKeyGenerator);
                    }
            );

            // GroupBy for reading
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMapForReading = new HashMap<>();
            if (isDistributed && !isProcessingOnExternalTime) {
                aggregationDurations.forEach(
                        incrementalDuration -> {
                            List<Expression> groupByExpressionList = new ArrayList<>();
                            Expression timestampExpression =
                                    AttributeFunction.function(
                                            "incrementalAggregator", "getAggregationStartTime",
                                            new Variable(AGG_START_TIMESTAMP_COL),
                                            new StringConstant(timeZone),
                                            new StringConstant(incrementalDuration.name())
                                    );
                            groupByExpressionList.add(timestampExpression);
                            if (isGroupBy) {
                                groupByExpressionList.addAll(groupByVariableList.stream()
                                        .map(groupByVariable -> (Expression) groupByVariable)
                                        .collect(Collectors.toList()));
                            }
                            GroupByKeyGenerator groupByKeyGenerator = new GroupByKeyGenerator(groupByExpressionList,
                                    processedMetaStreamEvent, SiddhiConstants.UNKNOWN_STATE, tableMap,
                                    processVariableExpressionExecutors, siddhiAppContext, aggregatorName);
                            groupByKeyGeneratorMapForReading.put(incrementalDuration, groupByKeyGenerator);
                        }

                );
            } else {
                groupByKeyGeneratorMapForReading.putAll(groupByKeyGeneratorMap);
            }

            // Create new scheduler
            EntryValveExecutor entryValveExecutor = new EntryValveExecutor(siddhiAppContext);
            LockWrapper lockWrapper = new LockWrapper(aggregatorName);
            lockWrapper.setLock(new ReentrantLock());

            Scheduler scheduler = SchedulerParser.parse(entryValveExecutor, siddhiAppContext);
            scheduler.init(lockWrapper, aggregatorName);
            scheduler.setStreamEventPool(new StreamEventPool(processedMetaStreamEvent, 10));

            QueryParserHelper.reduceMetaComplexEvent(incomingMetaStreamEvent);
            QueryParserHelper.reduceMetaComplexEvent(processedMetaStreamEvent);
            QueryParserHelper.updateVariablePosition(incomingMetaStreamEvent, incomingVariableExpressionExecutors);
            QueryParserHelper.updateVariablePosition(processedMetaStreamEvent, processVariableExpressionExecutors);


            Map<TimePeriod.Duration, Table> aggregationTables = initDefaultTables(aggregatorName, aggregationDurations,
                    processedMetaStreamEvent.getOutputStreamDefinition(), siddhiAppRuntimeBuilder,
                    aggregationDefinition.getAnnotations(), groupByVariableList, isProcessingOnExternalTime,
                    isDistributed);

            Map<TimePeriod.Duration, Executor> incrementalExecutorMap = buildIncrementalExecutors(
                    processedMetaStreamEvent, processExpressionExecutorsMap, groupByKeyGeneratorMap,
                    aggregationDurations, aggregationTables, siddhiAppContext,
                    aggregatorName, shouldUpdateTimestamp, timeZone, isPersistedAggregation,
                    incomingOutputStreamDefinition, isDistributed, shardId, isProcessingOnExternalTime,
                    aggregationDefinition, configManager, groupByVariableList);

            boolean isOptimisedLookup = aggregationTables.
                    get(aggregationDurations.get(0)) instanceof QueryableProcessor;

            List<String> groupByVariablesList = groupByVariableList.stream()
                    .map(Variable::getAttributeName)
                    .collect(Collectors.toList());

            List<OutputAttribute> defaultSelectorList = new ArrayList<>();
            if (isOptimisedLookup) {
                defaultSelectorList = incomingOutputStreamDefinition.getAttributeList().stream()
                        .map((attribute) -> new OutputAttribute(new Variable(attribute.getName())))
                        .collect(Collectors.toList());
            }

            if (isDebugEnabled) {
                log.debug("Optimised lookup mode is " + (isOptimisedLookup ? "enabled" : "disabled") + " for " +
                        "aggregation " + aggregatorName);
            }

            IncrementalDataPurger incrementalDataPurger = new IncrementalDataPurger();
            incrementalDataPurger.init(aggregationDefinition, new StreamEventPool(processedMetaStreamEvent, 10)
                    , aggregationTables, isProcessingOnExternalTime, siddhiAppContext, aggregationDurations, timeZone,
                    windowMap, aggregationMap);

            //Recreate in-memory data from tables
            IncrementalExecutorsInitialiser incrementalExecutorsInitialiser =
                    new IncrementalExecutorsInitialiser(aggregationDurations, aggregationTables, incrementalExecutorMap,
                            isDistributed, shardId, siddhiAppContext, processedMetaStreamEvent, tableMap, windowMap,
                            aggregationMap, timeZone);

            IncrementalExecutor rootIncrementalExecutor = (IncrementalExecutor) incrementalExecutorMap.
                    get(aggregationDurations.get(0));
            rootIncrementalExecutor.setScheduler(scheduler);
            // Connect entry valve to root incremental executor
            entryValveExecutor.setNextExecutor(rootIncrementalExecutor);

            QueryParserHelper.initStreamRuntime(streamRuntime, incomingMetaStreamEvent, lockWrapper, aggregatorName);

            LatencyTracker latencyTrackerFind = null;
            LatencyTracker latencyTrackerInsert = null;

            ThroughputTracker throughputTrackerFind = null;
            ThroughputTracker throughputTrackerInsert = null;

            if (siddhiAppContext.getStatisticsManager() != null) {
                latencyTrackerFind = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_FIND);
                latencyTrackerInsert = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_INSERT);

                throughputTrackerFind = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_FIND);
                throughputTrackerInsert = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_INSERT);
            }

            AggregationRuntime aggregationRuntime = new AggregationRuntime(aggregationDefinition,
                    isProcessingOnExternalTime, isDistributed, aggregationDurations,
                    incrementalExecutorMap, aggregationTables, outputExpressionExecutors,
                    processExpressionExecutorsMap, shouldUpdateTimestamp, groupByKeyGeneratorMapForReading,
                    isOptimisedLookup, defaultSelectorList, groupByVariablesList, isLatestEventColAdded,
                    baseAggregatorBeginIndex, finalBaseExpressions, incrementalDataPurger,
                    incrementalExecutorsInitialiser, ((SingleStreamRuntime) streamRuntime), siddhiAppContext,
                    processedMetaStreamEvent, latencyTrackerFind, throughputTrackerFind, timeZone);

            streamRuntime.setCommonProcessor(new IncrementalAggregationProcessor(aggregationRuntime,
                    incomingExpressionExecutors, processedMetaStreamEvent, latencyTrackerInsert,
                    throughputTrackerInsert, siddhiAppContext));

            return aggregationRuntime;

        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, aggregationDefinition, siddhiAppContext);
            throw t;
        }
    }

    private static String getTimeZone(SiddhiAppContext siddhiAppContext) {
        String timeZone = siddhiAppContext.getSiddhiContext().getConfigManager().extractProperty(SiddhiConstants
                .AGG_TIME_ZONE);
        if (timeZone == null) {
            return SiddhiConstants.AGG_TIME_ZONE_DEFAULT;
        }
        return timeZone;
    }

    private static Boolean validateTimeZone(String timeZone) {
        Set timeZoneSet = new HashSet(Arrays.asList(TimeZone.getAvailableIDs()));
        return timeZoneSet.contains(timeZone);
    }

    private static Map<TimePeriod.Duration, Executor> buildIncrementalExecutors(
            MetaStreamEvent processedMetaStreamEvent,
            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap,
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorList,
            List<TimePeriod.Duration> incrementalDurations,
            Map<TimePeriod.Duration, Table> aggregationTables, SiddhiAppContext siddhiAppContext,
            String aggregatorName, ExpressionExecutor shouldUpdateTimestamp, String timeZone,
            boolean isPersistedAggregation, StreamDefinition incomingOutputStreamDefinition, boolean isDistributed,
            String shardId, boolean isProcessingOnExternalTime, AggregationDefinition aggregationDefinition,
            ConfigManager configManager, List<Variable> groupByVariableList) {

        Map<TimePeriod.Duration, Executor> incrementalExecutorMap = new HashMap<>();
        Map<TimePeriod.Duration, Processor> cudProcessors;
        // Create incremental executors
        Executor child;
        Executor root = null;

        if (isPersistedAggregation) {
            cudProcessors = initAggregateQueryExecutor(incrementalDurations, processExpressionExecutorsMap,
                    incomingOutputStreamDefinition, isDistributed, shardId, isProcessingOnExternalTime,
                    siddhiAppContext, aggregationDefinition, configManager, aggregationTables, groupByVariableList);
            for (int i = incrementalDurations.size() - 1; i >= 0; i--) {
                // Base incremental expression executors created using new meta

                // Add an object to aggregationTable map inorder fill up the missing durations
                aggregationTables.putIfAbsent(incrementalDurations.get(i), null);
                boolean isRoot = false;
                if (i == 0) {
                    isRoot = true;
                }
                child = root;
                TimePeriod.Duration duration = incrementalDurations.get(i);
                Executor incrementalExecutor;
                if (duration == TimePeriod.Duration.SECONDS || duration == TimePeriod.Duration.MINUTES ||
                        duration == TimePeriod.Duration.HOURS) {
                    incrementalExecutor = new IncrementalExecutor(aggregatorName, duration,
                            getExpressionExecutorClones(processExpressionExecutorsMap.get(duration)),
                            getExpressionExecutorClone(shouldUpdateTimestamp),
                            groupByKeyGeneratorList.get(duration), processedMetaStreamEvent, child, isRoot,
                            aggregationTables.get(duration), siddhiAppContext, timeZone,
                            duration.equals(TimePeriod.Duration.HOURS));
                } else {
                    incrementalExecutor = new PersistedIncrementalExecutor(aggregatorName, duration,
                            processExpressionExecutorsMap.get(duration),
                            child, generateCUDMetaStreamEvent(isProcessingOnExternalTime), timeZone,
                            cudProcessors.get(duration), siddhiAppContext);
                }
                incrementalExecutorMap.put(duration, incrementalExecutor);
                root = incrementalExecutor;
            }
        } else {
            for (int i = incrementalDurations.size() - 1; i >= 0; i--) {
                // Base incremental expression executors created using new meta
                boolean isRoot = false;
                if (i == 0) {
                    isRoot = true;
                }
                child = root;
                TimePeriod.Duration duration = incrementalDurations.get(i);

                IncrementalExecutor incrementalExecutor = new IncrementalExecutor(aggregatorName, duration,
                        getExpressionExecutorClones(processExpressionExecutorsMap.get(duration)),
                        getExpressionExecutorClone(shouldUpdateTimestamp),
                        groupByKeyGeneratorList.get(duration), processedMetaStreamEvent, child, isRoot,
                        aggregationTables.get(duration), siddhiAppContext, timeZone, false);

                incrementalExecutorMap.put(duration, incrementalExecutor);
                root = incrementalExecutor;
            }
        }
        return incrementalExecutorMap;
    }

    private static List<ExpressionExecutor> constructProcessExpressionExecutors(
            SiddhiAppContext siddhiAppContext, Map<String, Table> tableMap, String aggregatorName,
            int baseAggregatorBeginIndex, List<Expression> finalBaseExpressions,
            StreamDefinition incomingOutputStreamDefinition, MetaStreamEvent processedMetaStreamEvent,
            List<VariableExpressionExecutor> processVariableExpressionExecutors, boolean groupBy,
            boolean isProcessingOnExternalTime, TimePeriod.Duration duration, boolean isDistributed, String shardId,
            boolean isLatestEventColAdded) {

        List<ExpressionExecutor> processExpressionExecutors = new ArrayList<>();
        List<Attribute> attributeList = incomingOutputStreamDefinition.getAttributeList();

        int i = 1;
        //Add timestamp executor
        Attribute attribute = attributeList.get(0);
        VariableExpressionExecutor variableExpressionExecutor = (VariableExpressionExecutor) ExpressionParser
                .parseExpression(new Variable(attribute.getName()), processedMetaStreamEvent, 0, tableMap,
                        processVariableExpressionExecutors, siddhiAppContext, groupBy, 0, aggregatorName);
        processExpressionExecutors.add(variableExpressionExecutor);

        if (isDistributed) {
            Expression shardIdExpression = Expression.value(shardId);
            ExpressionExecutor shardIdExpressionExecutor = ExpressionParser.parseExpression(shardIdExpression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors, siddhiAppContext,
                    groupBy, 0, aggregatorName);
            processExpressionExecutors.add(shardIdExpressionExecutor);
            i++;
        }

        if (isProcessingOnExternalTime) {
            Expression externalTimestampExpression =
                    AttributeFunction.function(
                            "incrementalAggregator", "getAggregationStartTime",
                            new Variable(AGG_EXTERNAL_TIMESTAMP_COL),
                            new StringConstant(duration.name())
                    );
            ExpressionExecutor externalTimestampExecutor = ExpressionParser.parseExpression(
                    externalTimestampExpression, processedMetaStreamEvent, 0, tableMap,
                    processVariableExpressionExecutors, siddhiAppContext, groupBy, 0, aggregatorName);
            processExpressionExecutors.add(externalTimestampExecutor);
            i++;
        }

        if (isLatestEventColAdded) {
            baseAggregatorBeginIndex = baseAggregatorBeginIndex - 1;
        }

        for (; i < baseAggregatorBeginIndex; i++) {
            attribute = attributeList.get(i);
            variableExpressionExecutor = (VariableExpressionExecutor) ExpressionParser.parseExpression(
                    new Variable(attribute.getName()), processedMetaStreamEvent, 0, tableMap,
                    processVariableExpressionExecutors, siddhiAppContext, groupBy, 0, aggregatorName);
            processExpressionExecutors.add(variableExpressionExecutor);
        }

        if (isLatestEventColAdded) {
            Expression lastTimestampExpression =
                    AttributeFunction.function(
                            "max",
                            new Variable(AGG_LAST_TIMESTAMP_COL)
                    );
            ExpressionExecutor latestTimestampExecutor = ExpressionParser.parseExpression(lastTimestampExpression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors, siddhiAppContext,
                    groupBy, 0, aggregatorName);
            processExpressionExecutors.add(latestTimestampExecutor);
        }

        for (Expression expression : finalBaseExpressions) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors, siddhiAppContext,
                    groupBy, 0, aggregatorName);
            processExpressionExecutors.add(expressionExecutor);
        }
        return processExpressionExecutors;
    }

    private static void processFinalBaseAggregators(
            SiddhiAppContext siddhiAppContext, Map<String, Table> tableMap,
            List<VariableExpressionExecutor> incomingVariableExpressionExecutors, String aggregatorName,
            MetaStreamEvent incomingMetaStreamEvent, List<ExpressionExecutor> incomingExpressionExecutors,
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
            List<Expression> finalBaseExpressions) {

        List<Attribute> finalBaseAttributes = new ArrayList<>();

        for (IncrementalAttributeAggregator incrementalAttributeAggregator : incrementalAttributeAggregators) {
            Attribute[] baseAttributes = incrementalAttributeAggregator.getBaseAttributes();
            Expression[] baseAttributeInitialValues = incrementalAttributeAggregator.getBaseAttributeInitialValues();
            Expression[] baseAggregators = incrementalAttributeAggregator.getBaseAggregators();

            for (int i = 0; i < baseAttributes.length; i++) {
                validateBaseAggregators(incrementalAttributeAggregators, incrementalAttributeAggregator, baseAttributes,
                        baseAttributeInitialValues, baseAggregators, i);

                if (!finalBaseAttributes.contains(baseAttributes[i])) {
                    finalBaseAttributes.add(baseAttributes[i]);
                    finalBaseExpressions.add(baseAggregators[i]);
                    incomingMetaStreamEvent.addOutputData(baseAttributes[i]);
                    incomingExpressionExecutors.add(ExpressionParser.parseExpression(baseAttributeInitialValues[i],
                            incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors,
                            siddhiAppContext, false, 0, aggregatorName));
                }
            }
        }
    }

    private static void validateBaseAggregators(List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
                                                IncrementalAttributeAggregator incrementalAttributeAggregator,
                                                Attribute[] baseAttributes, Expression[] baseAttributeInitialValues,
                                                Expression[] baseAggregators, int i) {
        for (int i1 = i; i1 < incrementalAttributeAggregators.size(); i1++) {
            IncrementalAttributeAggregator otherAttributeAggregator = incrementalAttributeAggregators.get(i1);
            if (otherAttributeAggregator != incrementalAttributeAggregator) {
                Attribute[] otherBaseAttributes = otherAttributeAggregator.getBaseAttributes();
                Expression[] otherBaseAttributeInitialValues = otherAttributeAggregator
                        .getBaseAttributeInitialValues();
                Expression[] otherBaseAggregators = otherAttributeAggregator.getBaseAggregators();
                for (int j = 0; j < otherBaseAttributes.length; j++) {
                    if (baseAttributes[i].equals(otherBaseAttributes[j])) {
                        if (!baseAttributeInitialValues[i].equals(otherBaseAttributeInitialValues[j])) {
                            throw new SiddhiAppCreationException("BaseAttributes having same name should " +
                                    "be defined with same initial values, but baseAttribute '" +
                                    baseAttributes[i] + "' is defined in '" +
                                    incrementalAttributeAggregator.getClass().getName() + "' and '" +
                                    otherAttributeAggregator.getClass().getName() +
                                    "' with different initial values.");
                        }
                        if (!baseAggregators[i].equals(otherBaseAggregators[j])) {
                            throw new SiddhiAppCreationException("BaseAttributes having same name should " +
                                    "be defined with same baseAggregators, but baseAttribute '" +
                                    baseAttributes[i] + "' is defined in '" +
                                    incrementalAttributeAggregator.getClass().getName() + "' and '" +
                                    otherAttributeAggregator.getClass().getName() +
                                    "' with different baseAggregators.");
                        }
                    }
                }
            }
        }
    }

    private static void populateIncomingAggregatorsAndExecutors(
            AggregationDefinition aggregationDefinition, SiddhiAppContext siddhiAppContext,
            Map<String, Table> tableMap, List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
            String aggregatorName, MetaStreamEvent incomingMetaStreamEvent,
            List<ExpressionExecutor> incomingExpressionExecutors,
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators, List<Variable> groupByVariableList,
            List<Expression> outputExpressions, boolean isProcessingOnExternalTime, boolean isDistributed,
            String shardId) {

        boolean isLatestEventAdded = false;

        ExpressionExecutor timestampExecutor = getTimeStampExecutor(siddhiAppContext, tableMap,
                incomingVariableExpressionExecutors, aggregatorName, incomingMetaStreamEvent);

        Attribute timestampAttribute = new Attribute(AGG_START_TIMESTAMP_COL, Attribute.Type.LONG);
        incomingMetaStreamEvent.addOutputData(timestampAttribute);
        incomingExpressionExecutors.add(timestampExecutor);

        if (isDistributed) {
            ExpressionExecutor nodeIdExpExecutor = new ConstantExpressionExecutor(shardId, Attribute.Type.STRING);
            incomingExpressionExecutors.add(nodeIdExpExecutor);
            incomingMetaStreamEvent.addOutputData(new Attribute(AGG_SHARD_ID_COL, Attribute.Type.STRING));
        }

        ExpressionExecutor externalTimestampExecutor = null;
        if (isProcessingOnExternalTime) {
            Expression externalTimestampExpression = aggregationDefinition.getAggregateAttribute();
            externalTimestampExecutor = ExpressionParser.parseExpression(externalTimestampExpression,
                    incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors, siddhiAppContext, false,
                    0, aggregatorName);

            if (externalTimestampExecutor.getReturnType() == Attribute.Type.STRING) {

                Expression expression = AttributeFunction.function("incrementalAggregator",
                        "timestampInMilliseconds", externalTimestampExpression);
                externalTimestampExecutor = ExpressionParser.parseExpression(expression, incomingMetaStreamEvent,
                        0, tableMap, incomingVariableExpressionExecutors, siddhiAppContext, false, 0, aggregatorName);

            } else if (externalTimestampExecutor.getReturnType() != Attribute.Type.LONG) {
                throw new SiddhiAppCreationException(
                        "AggregationDefinition '" + aggregationDefinition.getId() + "'s aggregateAttribute expects " +
                                "long or string, but found " + timestampExecutor.getReturnType() + ". " +
                                "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'",
                        externalTimestampExpression.getQueryContextStartIndex(),
                        externalTimestampExpression.getQueryContextEndIndex());
            }

            Attribute externalTimestampAttribute = new Attribute(AGG_EXTERNAL_TIMESTAMP_COL, Attribute.Type.LONG);
            incomingMetaStreamEvent.addOutputData(externalTimestampAttribute);
            incomingExpressionExecutors.add(externalTimestampExecutor);
        }

        AbstractDefinition incomingLastInputStreamDefinition = incomingMetaStreamEvent.getLastInputDefinition();
        for (Variable groupByVariable : groupByVariableList) {

            incomingMetaStreamEvent.addOutputData(incomingLastInputStreamDefinition.getAttributeList()
                    .get(incomingLastInputStreamDefinition.getAttributePosition(
                            groupByVariable.getAttributeName())));
            incomingExpressionExecutors.add(ExpressionParser.parseExpression(groupByVariable, incomingMetaStreamEvent,
                    0, tableMap, incomingVariableExpressionExecutors, siddhiAppContext, false, 0, aggregatorName));

        }

        // Add AGG_TIMESTAMP to output as well
        aggregationDefinition.getAttributeList().add(timestampAttribute);

        //Executors of time is differentiated with modes
        //check and set whether the aggregation in happened on an external timestamp
        if (isProcessingOnExternalTime) {
            outputExpressions.add(Expression.variable(AGG_EXTERNAL_TIMESTAMP_COL));
        } else {
            outputExpressions.add(Expression.variable(AGG_START_TIMESTAMP_COL));
        }

        for (OutputAttribute outputAttribute : aggregationDefinition.getSelector().getSelectionList()) {
            Expression expression = outputAttribute.getExpression();
            // If the select contains the aggregation function expression type will be AttributeFunction
            if (expression instanceof AttributeFunction) {
                IncrementalAttributeAggregator incrementalAggregator = null;
                try {
                    incrementalAggregator = (IncrementalAttributeAggregator)
                            SiddhiClassLoader.loadExtensionImplementation(
                                    new AttributeFunction("incrementalAggregator",
                                            ((AttributeFunction) expression).getName(),
                                            ((AttributeFunction) expression).getParameters()),
                                    IncrementalAttributeAggregatorExtensionHolder.getInstance(siddhiAppContext));
                } catch (SiddhiAppCreationException ex) {
                    try {
                        SiddhiClassLoader.loadExtensionImplementation((AttributeFunction) expression,
                                FunctionExecutorExtensionHolder.getInstance(siddhiAppContext));
                        processAggregationSelectors(aggregationDefinition, siddhiAppContext, tableMap,
                                incomingVariableExpressionExecutors, aggregatorName, incomingMetaStreamEvent,
                                incomingExpressionExecutors, outputExpressions, outputAttribute, expression);
                    } catch (SiddhiAppCreationException e) {
                        throw new SiddhiAppCreationException("'" + ((AttributeFunction) expression).getName() +
                                "' is neither a incremental attribute aggregator extension or a function" +
                                " extension", expression.getQueryContextStartIndex(),
                                expression.getQueryContextEndIndex());
                    }
                }
                if (incrementalAggregator != null) {
                    initIncrementalAttributeAggregator(incomingLastInputStreamDefinition,
                            (AttributeFunction) expression, incrementalAggregator);
                    incrementalAttributeAggregators.add(incrementalAggregator);
                    aggregationDefinition.getAttributeList().add(
                            new Attribute(outputAttribute.getRename(), incrementalAggregator.getReturnType()));
                    outputExpressions.add(incrementalAggregator.aggregate());
                }
            } else if (expression instanceof Variable && groupByVariableList.contains(expression)) {
                Attribute groupByAttribute = null;
                for (Attribute attribute : incomingMetaStreamEvent.getOutputData()) {
                    if (attribute.getName().equals(((Variable) expression).getAttributeName())) {
                        groupByAttribute = attribute;
                        break;
                    }
                }
                if (groupByAttribute == null) {
                    throw new SiddhiAppCreationException("Expected GroupBy attribute '" +
                            ((Variable) expression).getAttributeName() + "' not used in aggregation '" +
                            aggregatorName + "' processing.", expression.getQueryContextStartIndex(),
                            expression.getQueryContextEndIndex());
                }
                aggregationDefinition.getAttributeList().add(
                        new Attribute(outputAttribute.getRename(), groupByAttribute.getType()));
                outputExpressions.add(Expression.variable(groupByAttribute.getName()));

            } else {
                isLatestEventAdded = true;
                processAggregationSelectors(aggregationDefinition, siddhiAppContext, tableMap,
                        incomingVariableExpressionExecutors, aggregatorName, incomingMetaStreamEvent,
                        incomingExpressionExecutors, outputExpressions, outputAttribute, expression);
            }
        }

        if (isProcessingOnExternalTime && isLatestEventAdded) {
            Attribute lastEventTimeStamp = new Attribute(AGG_LAST_TIMESTAMP_COL, Attribute.Type.LONG);
            incomingMetaStreamEvent.addOutputData(lastEventTimeStamp);
            incomingExpressionExecutors.add(externalTimestampExecutor);
        }
    }

    private static void processAggregationSelectors(AggregationDefinition aggregationDefinition,
                                                    SiddhiAppContext siddhiAppContext, Map<String, Table> tableMap,
                                                    List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
                                                    String aggregatorName, MetaStreamEvent incomingMetaStreamEvent,
                                                    List<ExpressionExecutor> incomingExpressionExecutors,
                                                    List<Expression> outputExpressions, OutputAttribute outputAttribute,
                                                    Expression expression) {

        ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression, incomingMetaStreamEvent,
                0, tableMap, incomingVariableExpressionExecutors, siddhiAppContext, false, 0, aggregatorName);
        incomingExpressionExecutors.add(expressionExecutor);
        incomingMetaStreamEvent.addOutputData(
                new Attribute(outputAttribute.getRename(), expressionExecutor.getReturnType()));
        aggregationDefinition.getAttributeList().add(
                new Attribute(outputAttribute.getRename(), expressionExecutor.getReturnType()));
        outputExpressions.add(Expression.variable(outputAttribute.getRename()));
    }

    private static void initIncrementalAttributeAggregator(
            AbstractDefinition lastInputStreamDefinition, AttributeFunction attributeFunction,
            IncrementalAttributeAggregator incrementalAttributeAggregator) {

        String attributeName = null;
        Attribute.Type attributeType = null;
        if (attributeFunction.getParameters() != null && attributeFunction.getParameters()[0] != null) {
            if (attributeFunction.getParameters().length != 1) {
                throw new SiddhiAppCreationException("Incremental aggregator requires only one parameter. "
                        + "Found " + attributeFunction.getParameters().length,
                        attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
            }
            if (!(attributeFunction.getParameters()[0] instanceof Variable)) {
                throw new SiddhiAppCreationException("Incremental aggregator expected a variable. " +
                        "However a parameter of type " + attributeFunction.getParameters()[0].getClass().getTypeName()
                        + " was found",
                        attributeFunction.getParameters()[0].getQueryContextStartIndex(),
                        attributeFunction.getParameters()[0].getQueryContextEndIndex());
            }
            attributeName = ((Variable) attributeFunction.getParameters()[0]).getAttributeName();
            attributeType = lastInputStreamDefinition.getAttributeType(attributeName);
        }

        //This will initialize the aggregation function(avg, count, sum, etc) and figure out the parameters that needed
        //to store in table eg: count() => AGG_COUNT
        incrementalAttributeAggregator.init(attributeName, attributeType);

        Attribute[] baseAttributes = incrementalAttributeAggregator.getBaseAttributes();
        Expression[] baseAttributeInitialValues = incrementalAttributeAggregator
                .getBaseAttributeInitialValues();
        Expression[] baseAggregators = incrementalAttributeAggregator.getBaseAggregators();

        if (baseAttributes.length != baseAggregators.length) {
            throw new SiddhiAppCreationException("Number of baseAggregators '" +
                    baseAggregators.length + "' and baseAttributes '" +
                    baseAttributes.length + "' is not equal for '" + attributeFunction + "'",
                    attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
        }
        if (baseAttributeInitialValues.length != baseAggregators.length) {
            throw new SiddhiAppCreationException("Number of baseAggregators '" +
                    baseAggregators.length + "' and baseAttributeInitialValues '" +
                    baseAttributeInitialValues.length + "' is not equal for '" +
                    attributeFunction + "'",
                    attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
        }
    }

    private static ExpressionExecutor getTimeStampExecutor(
            SiddhiAppContext siddhiAppContext, Map<String, Table> tableMap,
            List<VariableExpressionExecutor> variableExpressionExecutors, String aggregatorName,
            MetaStreamEvent metaStreamEvent) {

        Expression timestampExpression;
        ExpressionExecutor timestampExecutor;

        // Execution is based on system time, the GMT time zone would be used.
        timestampExpression = AttributeFunction.function("currentTimeMillis", null);
        timestampExecutor = ExpressionParser.parseExpression(timestampExpression,
                metaStreamEvent, 0, tableMap, variableExpressionExecutors,
                siddhiAppContext, false, 0, aggregatorName);
        return timestampExecutor;
    }

    private static List<TimePeriod.Duration> getSortedPeriods(TimePeriod timePeriod, boolean isPersistedAggregation) {
        try {
            List<TimePeriod.Duration> durations = timePeriod.getDurations();
            if (isRange(timePeriod)) {
                durations = fillGap(durations.get(0), durations.get(1));
            }
            return sortedDurations(durations, isPersistedAggregation);
        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, timePeriod, null);
            throw t;
        }
    }

    private static List<TimePeriod.Duration> sortedDurations(List<TimePeriod.Duration> durations,
                                                             boolean isPersistedAggregation) {
        List<TimePeriod.Duration> copyDurations = new ArrayList<>(durations);

        Comparator periodComparator = (Comparator<TimePeriod.Duration>) (firstDuration, secondDuration) -> {
            int firstOrdinal = firstDuration.ordinal();
            int secondOrdinal = secondDuration.ordinal();
            if (firstOrdinal > secondOrdinal) {
                return 1;
            } else if (firstOrdinal < secondOrdinal) {
                return -1;
            }
            return 0;
        };
        copyDurations.sort(periodComparator);
        if (isPersistedAggregation && copyDurations.get(0).ordinal() >= 3) {
            copyDurations = fillGap(TimePeriod.Duration.HOURS, copyDurations.get(copyDurations.size() - 1));
        }
        return copyDurations;
    }

    private static boolean isRange(TimePeriod timePeriod) {
        return timePeriod.getOperator() == TimePeriod.Operator.RANGE;
    }

    private static List<TimePeriod.Duration> fillGap(TimePeriod.Duration start, TimePeriod.Duration end) {
        TimePeriod.Duration[] durations = TimePeriod.Duration.values();
        List<TimePeriod.Duration> filledDurations = new ArrayList<>();

        int startIndex = start.ordinal();
        int endIndex = end.ordinal();

        if (startIndex > endIndex) {
            throw new SiddhiAppCreationException(
                    "Start time period must be less than end time period for range aggregation calculation");
        }

        if (startIndex == endIndex) {
            filledDurations.add(start);
        } else {
            TimePeriod.Duration[] temp = new TimePeriod.Duration[endIndex - startIndex + 1];
            System.arraycopy(durations, startIndex, temp, 0, endIndex - startIndex + 1);
            filledDurations = Arrays.asList(temp);
        }
        return filledDurations;
    }

    private static HashMap<TimePeriod.Duration, Table> initDefaultTables(
            String aggregatorName, List<TimePeriod.Duration> aggregationDurations,
            StreamDefinition streamDefinition, SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
            List<Annotation> annotations, List<Variable> groupByVariableList, boolean isProcessingOnExternalTime,
            boolean enablePartitioning) {

        HashMap<TimePeriod.Duration, Table> aggregationTableMap = new HashMap<>();

        // Create annotations for primary key
        Annotation primaryKeyAnnotation = new Annotation(ANNOTATION_PRIMARY_KEY);
        primaryKeyAnnotation.element(null, AGG_START_TIMESTAMP_COL);

        if (enablePartitioning) {
            primaryKeyAnnotation.element(null, AGG_SHARD_ID_COL);
        }
        if (isProcessingOnExternalTime) {
            primaryKeyAnnotation.element(null, AGG_EXTERNAL_TIMESTAMP_COL);
        }
        for (Variable groupByVariable : groupByVariableList) {
            primaryKeyAnnotation.element(null, groupByVariable.getAttributeName());
        }
        annotations.add(primaryKeyAnnotation);
        for (TimePeriod.Duration duration : aggregationDurations) {
            String tableId = aggregatorName + "_" + duration.toString();
            TableDefinition tableDefinition = TableDefinition.id(tableId);
            for (Attribute attribute : streamDefinition.getAttributeList()) {
                tableDefinition.attribute(attribute.getName(), attribute.getType());
            }
            annotations.forEach(tableDefinition::annotation);
            siddhiAppRuntimeBuilder.defineTable(tableDefinition);
            aggregationTableMap.put(duration, siddhiAppRuntimeBuilder.getTableMap().get(tableId));
        }
        return aggregationTableMap;
    }

    private static Map<TimePeriod.Duration, Processor> initAggregateQueryExecutor(
            List<TimePeriod.Duration> aggregationDurations,
            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap,
            StreamDefinition incomingOutputStreamDefinition, boolean isDistributed,
            String shardID, boolean isProcessingOnExternalTime, SiddhiAppContext siddhiAppContext,
            AggregationDefinition aggregationDefinition, ConfigManager configManager,
            Map<TimePeriod.Duration, Table> aggregationTables, List<Variable> groupByVariableList) {

        Map<TimePeriod.Duration, Processor> cudProcessors = new LinkedHashMap<>();
        String datasourceName = AnnotationHelper.getAnnotationElement(SiddhiConstants.NAMESPACE_STORE,
                "datasource", aggregationDefinition.getAnnotations()).getValue();
        if (datasourceName == null || datasourceName.isEmpty()) {
            throw new SiddhiAppCreationException("Datasource configuration must be provided inorder to use persisted " +
                    "aggregation mode");
        }
        Database databaseType = getDatabaseType(configManager, datasourceName);

        SiddhiAppContext cudSiddhiAppContext = new SiddhiAppContext();
        SiddhiContext context = new SiddhiContext();
        context.setConfigManager(configManager);
        cudSiddhiAppContext.setSiddhiContext(context);
        StringConstant datasource = new StringConstant(datasourceName);
        ConstantExpressionExecutor datasourceExecutor = new ConstantExpressionExecutor(datasource.getValue(),
                Attribute.Type.STRING);
        Expression[] streamHandler;
        ExpressionExecutor[] cudStreamProcessorInputVariables;
        if (isProcessingOnExternalTime) {
            streamHandler = new Expression[7];
        } else {
            streamHandler = new Expression[5];
        }
        try {
            DBAggregationQueryConfigurationEntry dbAggregationQueryConfigurationEntry = DBAggregationQueryUtil.
                    lookupCurrentQueryConfigurationEntry(databaseType);

            for (int i = aggregationDurations.size() - 1; i > 0; i--) {
                if (aggregationDurations.get(i).ordinal() >= 3) {
                    log.debug(" Initializing cudProcessors for durations ");
                    String databaseSelectQuery = generateDatabaseQuery(processExpressionExecutorsMap.
                                    get(aggregationDurations.get(i)), dbAggregationQueryConfigurationEntry,
                            incomingOutputStreamDefinition, isDistributed, shardID, isProcessingOnExternalTime,
                            aggregationTables.get(aggregationDurations.get(i)),
                            aggregationTables.get(aggregationDurations.get(i - 1)), groupByVariableList,
                            aggregationDurations.get(i));
                    StringConstant selectQuery = new StringConstant(databaseSelectQuery);
                    ConstantExpressionExecutor selectExecutor = new ConstantExpressionExecutor(selectQuery.getValue(),
                            Attribute.Type.STRING);
                    Map<Attribute, int[]> cudInputStreamAttributesMap =
                            generateCUDInputStreamAttributes(isProcessingOnExternalTime);
                    if (isProcessingOnExternalTime) {
                        cudStreamProcessorInputVariables = new ExpressionExecutor[7];
                    } else {
                        cudStreamProcessorInputVariables = new ExpressionExecutor[5];
                    }
                    cudStreamProcessorInputVariables[0] = datasourceExecutor;
                    cudStreamProcessorInputVariables[1] = selectExecutor;
                    streamHandler[0] = datasource;
                    streamHandler[1] = selectQuery;
                    MetaStreamEvent metaStreamEvent = generateCUDMetaStreamEvent(isProcessingOnExternalTime);
                    StreamDefinition outputStream = new StreamDefinition();
                    VariableExpressionExecutor variableExpressionExecutor;
                    int j = 0;
                    for (Map.Entry<Attribute, int[]> entry : cudInputStreamAttributesMap.entrySet()) {
                        Attribute attribute = entry.getKey();
                        Variable timestampVariable = new Variable(attribute.getName());
                        for (int position : entry.getValue()) {
                            streamHandler[position + 2] = timestampVariable;
                            variableExpressionExecutor = new VariableExpressionExecutor(attribute, 0, 0);
                            variableExpressionExecutor.setPosition(new int[]{2, j});
                            cudStreamProcessorInputVariables[position + 2] = variableExpressionExecutor;
                        }
                        outputStream.attribute(attribute.getName(), attribute.getType());
                        j++;
                    }

                    StreamFunction cudStreamFunction = new StreamFunction(NAMESPACE_RDBMS, FUNCTION_NAME_CUD,
                            streamHandler);

                    cudProcessors.put(aggregationDurations.get(i), getCudProcessor(cudStreamFunction, siddhiAppContext,
                            metaStreamEvent, cudStreamProcessorInputVariables, aggregationDurations.get(i)));
                }
            }
            return cudProcessors;
        } catch (CannotLoadConfigurationException e) {
            throw new SiddhiAppCreationException("Error occurred while initializing the persisted incremental " +
                    "aggregation. Could not load the db quires for database type " + databaseType);
        }
    }

    private static MetaStreamEvent generateCUDMetaStreamEvent(boolean isProcessingOnExternalTime) {
        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        Map<Attribute, int[]> cudInputStreamAttributesList =
                generateCUDInputStreamAttributes(isProcessingOnExternalTime);
        StreamDefinition inputDefinition = new StreamDefinition();
        inputDefinition.setId("inputStream");
        Attribute numRecordsAtt = new Attribute("numRecords", Attribute.Type.INT);
        for (Attribute attribute : cudInputStreamAttributesList.keySet()) {
            metaStreamEvent.addData(attribute);
            inputDefinition.attribute(attribute.getName(), attribute.getType());
            metaStreamEvent.addOutputData(attribute);
        }
        metaStreamEvent.addInputDefinition(inputDefinition);
        metaStreamEvent.addOutputData(numRecordsAtt);
        metaStreamEvent.setEventType(MetaStreamEvent.EventType.DEFAULT);
        return metaStreamEvent;
    }

    private static String generateDatabaseQuery(List<ExpressionExecutor> expressionExecutors,
                                                DBAggregationQueryConfigurationEntry
                                                        dbAggregationQueryConfigurationEntry,
                                                StreamDefinition incomingOutputStreamDefinition,
                                                boolean isDistributed, String shardID,
                                                boolean isProcessingOnExternalTime, Table aggregationTable,
                                                Table parentAggregationTable, List<Variable> groupByVariableList,
                                                TimePeriod.Duration duration) {

        DBAggregationSelectFunctionTemplate dbAggregationSelectFunctionTemplates = dbAggregationQueryConfigurationEntry.
                getRdbmsSelectFunctionTemplate();
        DBAggregationSelectQueryTemplate dbAggregationSelectQueryTemplate = dbAggregationQueryConfigurationEntry.
                getRdbmsSelectQueryTemplate();
        DBAggregationTimeConversionDurationMapping dbAggregationTimeConversionDurationMapping =
                dbAggregationQueryConfigurationEntry.getDbAggregationTimeConversionDurationMapping();
        List<Attribute> attributeList = incomingOutputStreamDefinition.getAttributeList();
        List<String> groupByColumnNames = new ArrayList<>();

        StringJoiner outerSelectColumnJoiner = new StringJoiner(", ");
        StringJoiner subSelectT1ColumnJoiner = new StringJoiner(", ", SQL_SELECT, " ");
        StringJoiner subSelectT2ColumnJoiner = new StringJoiner(", ");
        StringJoiner innerSelectT2ColumnJoiner = new StringJoiner(", ", SQL_SELECT, " ");

        StringJoiner onConditionBuilder = new StringJoiner(SQL_AND);
        StringJoiner subSelectT2OnConditionBuilder = new StringJoiner(SQL_AND);

        StringJoiner groupByQueryBuilder = new StringJoiner(", ");
        StringJoiner finalSelectQuery = new StringJoiner(" ");
        StringJoiner completeQuery = new StringJoiner(" ");
        StringJoiner insertIntoColumns = new StringJoiner(", ");
        StringBuilder filterQueryBuilder = new StringBuilder();
        StringBuilder insertIntoQueryBuilder = new StringBuilder();

        String innerFromClause = SQL_FROM + parentAggregationTable.getTableDefinition().getId();
        String innerWhereFilterClause;
        String groupByClause;
        String innerT2WhereCondition;

        StringJoiner innerT2Query = new StringJoiner(" ");
        StringJoiner subQueryT1 = new StringJoiner(" ");
        StringJoiner subQueryT2 = new StringJoiner(" ");

        attributeList.stream().forEach(attribute -> insertIntoColumns.add(attribute.getName()));
        int i = 0;
        insertIntoQueryBuilder.append(dbAggregationSelectQueryTemplate.getRecordInsertQuery().
                replace(PLACEHOLDER_TABLE_NAME, aggregationTable.getTableDefinition().getId()).
                replace(PLACEHOLDER_COLUMNS, insertIntoColumns.toString()));

        filterQueryBuilder.append(" (").append(AGG_START_TIMESTAMP_COL).append(" >= ?").append(" AND ")
                .append(AGG_START_TIMESTAMP_COL).append(" < ? ").append(") ");

        if (isDistributed) {
            filterQueryBuilder.append(" AND ").append(AGG_SHARD_ID_COL).append(" = '").append(shardID).append("' ");
            groupByQueryBuilder.add(AGG_SHARD_ID_COL);
            if (isProcessingOnExternalTime) {
                subSelectT1ColumnJoiner.add(AGG_SHARD_ID_COL);
            }
        }

        if (isProcessingOnExternalTime) {
            groupByVariableList.stream().forEach(variable -> {
                groupByColumnNames.add(variable.getAttributeName());
                groupByQueryBuilder.add(variable.getAttributeName());
                onConditionBuilder.add(SUB_SELECT_QUERY_REF_T1 + "." + variable.getAttributeName() +
                        EQUALS + SUB_SELECT_QUERY_REF_T2 + "." + variable.getAttributeName());
                subSelectT2OnConditionBuilder.add(parentAggregationTable.getTableDefinition().getId() + "." +
                        variable.getAttributeName() + EQUALS + INNER_SELECT_QUERY_REF_T3 + "." +
                        variable.getAttributeName());
            });

            innerT2WhereCondition = INNER_SELECT_QUERY_REF_T3 + "." + groupByVariableList.get(0).getAttributeName() +
                    SQL_NOT_NULL;

            for (ExpressionExecutor expressionExecutor : expressionExecutors) {

                if (expressionExecutor instanceof VariableExpressionExecutor) {
                    VariableExpressionExecutor variableExpressionExecutor = (VariableExpressionExecutor) expressionExecutor;
                    if (variableExpressionExecutor.getAttribute().getName()
                            .equals(AGG_START_TIMESTAMP_COL)) {
                        outerSelectColumnJoiner.add(" ? " + SQL_AS + variableExpressionExecutor.getAttribute().getName());
                    } else if (!variableExpressionExecutor.getAttribute().getName().equals(AGG_EXTERNAL_TIMESTAMP_COL)) {
                        if (groupByColumnNames.contains(variableExpressionExecutor.getAttribute().getName())) {
                            subSelectT2ColumnJoiner.add(INNER_SELECT_QUERY_REF_T3 + "." +
                                    variableExpressionExecutor.getAttribute().getName() + SQL_AS +
                                    variableExpressionExecutor.getAttribute().getName()) ;
                            outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T1 + "." +
                                    variableExpressionExecutor.getAttribute().getName() +
                                    SQL_AS + attributeList.get(i).getName());
                            subSelectT1ColumnJoiner.add(variableExpressionExecutor.getAttribute().getName());
                            innerSelectT2ColumnJoiner.add(variableExpressionExecutor.getAttribute().getName());
                        } else {
                            subSelectT2ColumnJoiner.add(variableExpressionExecutor.getAttribute().getName());
                            outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T2 + "." +
                                    variableExpressionExecutor.getAttribute().getName() +
                                    SQL_AS + attributeList.get(i).getName());
                        }
                    }
                } else if (expressionExecutor instanceof IncrementalAggregateBaseTimeFunctionExecutor) {
                    if (attributeList.get(i).getName().equals(AGG_EXTERNAL_TIMESTAMP_COL)) {
                        outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T1 + "." + AGG_EXTERNAL_TIMESTAMP_COL + SQL_AS +
                                AGG_EXTERNAL_TIMESTAMP_COL);
                        subSelectT1ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getTimeConversionFunction().
                                replace(PLACEHOLDER_COLUMN, AGG_EXTERNAL_TIMESTAMP_COL).replace(PLACEHOLDER_DURATION,
                                dbAggregationTimeConversionDurationMapping.getDurationMapping(duration))
                                + SQL_AS + AGG_EXTERNAL_TIMESTAMP_COL);
                        subSelectT2ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getTimeConversionFunction().
                                replace(PLACEHOLDER_COLUMN, AGG_EXTERNAL_TIMESTAMP_COL).replace(PLACEHOLDER_DURATION,
                                dbAggregationTimeConversionDurationMapping.getDurationMapping(duration))
                                + SQL_AS + AGG_EXTERNAL_TIMESTAMP_COL);
                        onConditionBuilder.add(SUB_SELECT_QUERY_REF_T1 + "." + AGG_EXTERNAL_TIMESTAMP_COL + EQUALS +
                                SUB_SELECT_QUERY_REF_T2 + "." + AGG_EXTERNAL_TIMESTAMP_COL);
                    } else {
                        outerSelectColumnJoiner.add(" ? " + SQL_AS + attributeList.get(i).getName());
                    }
                } else if (expressionExecutor instanceof ConstantExpressionExecutor) {
                    outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T1 + "." + attributeList.get(i).getName() +
                            SQL_AS + attributeList.get(i).getName());
                } else if (expressionExecutor instanceof GroupByAggregationAttributeExecutor) {
                    if (((GroupByAggregationAttributeExecutor) expressionExecutor).getAttributeAggregator() instanceof
                            MaxAttributeAggregator) {
                        if (attributeList.get(i).getName().equals(AGG_LAST_TIMESTAMP_COL)) {
                            innerSelectT2ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getMaxFunction().
                                    replace(PLACEHOLDER_COLUMN, attributeList.get(i).getName()) + SQL_AS +
                                    attributeList.get(i).getName());
                            subSelectT2ColumnJoiner.add(INNER_SELECT_QUERY_REF_T3 + "." + attributeList.get(i).getName() +
                                    SQL_AS + attributeList.get(i).getName());
                            outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T2 + "." + attributeList.get(i).getName() +
                                    SQL_AS + attributeList.get(i).getName());
                            subSelectT2OnConditionBuilder.add(parentAggregationTable.getTableDefinition().getId() + "." +
                                    attributeList.get(i).getName() + EQUALS + INNER_SELECT_QUERY_REF_T3 + "." +
                                    attributeList.get(i).getName());
                        } else {
                            outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T1 + "." + attributeList.get(i).getName() + SQL_AS +
                                    attributeList.get(i).getName());
                            subSelectT1ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getMaxFunction().replace(
                                    PLACEHOLDER_COLUMN, attributeList.get(i).getName()) + SQL_AS +
                                    attributeList.get(i).getName());
                        }
                    } else if (((GroupByAggregationAttributeExecutor) expressionExecutor).getAttributeAggregator() instanceof
                            MinAttributeAggregator) {
                        outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T1 + "." + attributeList.get(i).getName() + SQL_AS +
                                attributeList.get(i).getName());
                        subSelectT1ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getMinFunction().replace(
                                PLACEHOLDER_COLUMN, attributeList.get(i).getName()) + SQL_AS +
                                attributeList.get(i).getName());
                    } else if (((GroupByAggregationAttributeExecutor) expressionExecutor).getAttributeAggregator() instanceof
                            SumAttributeAggregator) {
                        outerSelectColumnJoiner.add(SUB_SELECT_QUERY_REF_T1 + "." + attributeList.get(i).getName() +
                                SQL_AS + attributeList.get(i).getName());
                        subSelectT1ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getSumFunction().replace(
                                PLACEHOLDER_COLUMN, attributeList.get(i).getName()) +
                                SQL_AS + attributeList.get(i).getName());
                    }
                }
                i++;
            }

            groupByQueryBuilder.add(dbAggregationSelectFunctionTemplates.getTimeConversionFunction().
                    replace(PLACEHOLDER_COLUMN, AGG_EXTERNAL_TIMESTAMP_COL).replace(PLACEHOLDER_DURATION,
                    dbAggregationTimeConversionDurationMapping.getDurationMapping(duration)));

            groupByClause = dbAggregationSelectQueryTemplate.getGroupByClause().replace(PLACEHOLDER_COLUMNS,
                    groupByQueryBuilder.toString());

            innerWhereFilterClause = dbAggregationSelectQueryTemplate.getWhereClause().
                    replace(PLACEHOLDER_CONDITION, filterQueryBuilder.toString());
            innerT2Query.add(innerSelectT2ColumnJoiner.toString()).add(innerFromClause).
                    add(innerWhereFilterClause).add(groupByClause);

            subQueryT1.add(subSelectT1ColumnJoiner.toString()).add(innerFromClause).
                    add(innerWhereFilterClause).add(groupByClause);

            subQueryT2.add(dbAggregationSelectQueryTemplate.getSelectQueryWithInnerSelect().
                    replace(PLACEHOLDER_SELECTORS, subSelectT2ColumnJoiner.toString()).
                    replace(PLACEHOLDER_TABLE_NAME, parentAggregationTable.getTableDefinition().getId()).
                    replace(PLACEHOLDER_INNER_QUERY_2, innerT2Query.toString()).
                    replace(PLACEHOLDER_ON_CONDITION, subSelectT2OnConditionBuilder.toString()).
                    replace(PLACEHOLDER_CONDITION, innerT2WhereCondition));

            finalSelectQuery.add(dbAggregationSelectQueryTemplate.getJoinClause().
                    replace(PLACEHOLDER_SELECTORS, outerSelectColumnJoiner.toString()).
                    replace(PLACEHOLDER_FROM_CONDITION, subQueryT1.toString()).
                    replace(PLACEHOLDER_INNER_QUERY_1, subQueryT2.toString()).
                    replace(PLACEHOLDER_CONDITION, onConditionBuilder.toString()));

            completeQuery.add(insertIntoQueryBuilder.toString()).add(finalSelectQuery.toString());

        } else {

            for (ExpressionExecutor executor : expressionExecutors) {
                if (executor instanceof VariableExpressionExecutor) {
                    VariableExpressionExecutor variableExpressionExecutor = (VariableExpressionExecutor) executor;
                    if (variableExpressionExecutor.getAttribute().getName().equals(AGG_START_TIMESTAMP_COL)) {
                        subSelectT1ColumnJoiner.add("? " + SQL_AS + variableExpressionExecutor.
                                getAttribute().getName());
                    } else {
                        subSelectT1ColumnJoiner.add(variableExpressionExecutor.getAttribute().getName());
                        groupByQueryBuilder.add(variableExpressionExecutor.getAttribute().getName());
                    }
                } else if (executor instanceof ConstantExpressionExecutor) {
                    if (((ConstantExpressionExecutor) executor).getValue() != null) {
                        subSelectT1ColumnJoiner.add("'" + ((ConstantExpressionExecutor) executor).getValue() + "' " +
                                SQL_AS + attributeList.get(i).getName());
                    } else {
                        subSelectT1ColumnJoiner.add(attributeList.get(i).getName());
                    }
                } else if (executor instanceof GroupByAggregationAttributeExecutor) {
                    if (((GroupByAggregationAttributeExecutor) executor).getAttributeAggregator() instanceof
                            SumAttributeAggregator) {
                        subSelectT1ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getSumFunction().
                                replace(PLACEHOLDER_COLUMN, attributeList.get(i).getName()).concat(SQL_AS).
                                concat(attributeList.get(i).getName()));
                    } else if (((GroupByAggregationAttributeExecutor) executor).getAttributeAggregator() instanceof
                            MinAttributeAggregator) {
                        subSelectT1ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getMinFunction().
                                replace(PLACEHOLDER_COLUMN, attributeList.get(i).getName()).concat(SQL_AS).
                                concat(attributeList.get(i).getName()));
                    } else if (((GroupByAggregationAttributeExecutor) executor).getAttributeAggregator() instanceof
                            MaxAttributeAggregator) {
                        subSelectT1ColumnJoiner.add(dbAggregationSelectFunctionTemplates.getMaxFunction().
                                replace(PLACEHOLDER_COLUMN, attributeList.get(i).getName()).concat(SQL_AS).
                                concat(attributeList.get(i).getName()));
                    }
                }
                i++;
            }

            completeQuery.add(insertIntoQueryBuilder.toString()).add(subSelectT1ColumnJoiner.toString()).add(innerFromClause).
                    add(SQL_WHERE + filterQueryBuilder).add(dbAggregationSelectQueryTemplate.getGroupByClause().
                    replace(PLACEHOLDER_COLUMNS, groupByQueryBuilder.toString()));
        }
        return completeQuery.toString();

    }

    private static Map<Attribute, int[]> generateCUDInputStreamAttributes(boolean isProcessingOnExternalTime) {
        Map<Attribute, int[]> cudInputStreamAttributeList = new LinkedHashMap<>();
        if (isProcessingOnExternalTime) {
            cudInputStreamAttributeList.put(new Attribute(FROM_TIMESTAMP, Attribute.Type.LONG), new int[]{0, 1, 3});
            cudInputStreamAttributeList.put(new Attribute(TO_TIMESTAMP, Attribute.Type.LONG), new int[]{2, 4});
        } else {
            cudInputStreamAttributeList.put(new Attribute(FROM_TIMESTAMP, Attribute.Type.LONG), new int[]{0, 1});
            cudInputStreamAttributeList.put(new Attribute(TO_TIMESTAMP, Attribute.Type.LONG), new int[]{2});
        }
        return cudInputStreamAttributeList;
    }

    private static Database getDatabaseType(ConfigManager configManager, String datasourceName) {
        ConfigReader configReader = configManager.generateConfigReader("wso2.datasources", datasourceName);
        String databaseType = configReader.readConfig("driverClassName", "null").toLowerCase();
        if (databaseType.contains("mysql")) {
            return Database.MYSQL;
        } else if (databaseType.contains("oracle")) {
            return Database.ORACLE;
        } else if (databaseType.contains("mssql") || databaseType.contains("sqlserver")) {
            return Database.MSSQL;
        } else if (databaseType.contains("postgres")) {
            return Database.PostgreSQL;
        } else {
            log.warn("Provided database type " + databaseType + "is not recognized as a supported database type for" +
                    " persisted incremental aggregation, using MySQL as default ");
            return Database.MYSQL;
        }
    }

    private static Processor getCudProcessor(StreamHandler streamHandler, SiddhiAppContext siddhiAppContext,
                                             MetaStreamEvent metaStreamEvent,
                                             ExpressionExecutor[] attributeExpressionExecutors,
                                             TimePeriod.Duration duration) {
        ConfigReader configReader = siddhiAppContext.getSiddhiContext().getConfigManager().
                generateConfigReader(((StreamFunction) streamHandler).getNamespace(),
                        ((StreamFunction) streamHandler).getName());

        AbstractStreamProcessor abstractStreamProcessor;
        abstractStreamProcessor = (StreamProcessor) SiddhiClassLoader.loadExtensionImplementation(
                (Extension) streamHandler,
                StreamProcessorExtensionHolder.getInstance(siddhiAppContext));
        abstractStreamProcessor.initProcessor(metaStreamEvent.getLastInputDefinition(), attributeExpressionExecutors
                , configReader, siddhiAppContext, false,
                metaStreamEvent.getLastInputDefinition().getId(), streamHandler);

        if (metaStreamEvent.getInputDefinitions().size() == 2) {
            AbstractDefinition outputDefinition = metaStreamEvent.getInputDefinitions().get(1);
            List<Attribute> outputAttributes = outputDefinition.getAttributeList();
            for (Attribute attribute : outputAttributes) {
                metaStreamEvent.addOutputData(attribute);
            }
            metaStreamEvent.setOutputDefinition((StreamDefinition) outputDefinition);
        }
        abstractStreamProcessor.constructStreamEventPopulater(metaStreamEvent, 0);
        abstractStreamProcessor.setNextProcessor(new PersistedAggregationResultsProcessor(duration));

        return abstractStreamProcessor;
    }

    public enum Database {
        MYSQL,
        ORACLE,
        MSSQL,
        DB2,
        PostgreSQL,
        H2,
        DEFAULT
    }
}
