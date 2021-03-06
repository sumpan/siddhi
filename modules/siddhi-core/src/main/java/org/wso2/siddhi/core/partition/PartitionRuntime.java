/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.siddhi.core.partition;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.partition.executor.PartitionExecutor;
import org.wso2.siddhi.core.query.QueryRuntime;
import org.wso2.siddhi.core.query.input.stream.join.JoinStreamRuntime;
import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import org.wso2.siddhi.core.query.input.stream.state.StateStreamRuntime;
import org.wso2.siddhi.core.query.output.callback.InsertIntoStreamCallback;
import org.wso2.siddhi.core.stream.StreamJunction;
import org.wso2.siddhi.core.util.parser.helper.DefinitionParserHelper;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.DuplicateAnnotationException;
import org.wso2.siddhi.query.api.execution.partition.Partition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.state.*;
import org.wso2.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.InsertIntoStream;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PartitionRuntime implements Snapshotable {


    private String partitionId;
    private String elementId;
    private Partition partition;
    private ConcurrentMap<String, StreamJunction> localStreamJunctionMap = new ConcurrentHashMap<String, StreamJunction>(); //contains definition
    private ConcurrentMap<String, AbstractDefinition> localStreamDefinitionMap = new ConcurrentHashMap<String, AbstractDefinition>(); //contains stream definition
    private ConcurrentMap<String, AbstractDefinition> streamDefinitionMap;
    private ConcurrentMap<String, StreamJunction> streamJunctionMap;
    private ConcurrentMap<String, QueryRuntime> metaQueryRuntimeMap = new ConcurrentHashMap<String, QueryRuntime>();
    private List<PartitionInstanceRuntime> partitionInstanceRuntimeList = new ArrayList<PartitionInstanceRuntime>();
    private ConcurrentMap<String, PartitionStreamReceiver> partitionStreamReceivers = new ConcurrentHashMap<String, PartitionStreamReceiver>();
    private ExecutionPlanContext executionPlanContext;

    public PartitionRuntime(ConcurrentMap<String, AbstractDefinition> streamDefinitionMap,ConcurrentMap<String, StreamJunction> streamJunctionMap, Partition partition, ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        try {
            Element element = AnnotationHelper.getAnnotationElement("info", "name", partition.getAnnotations());
            if (element != null) {
                this.partitionId = element.getValue();
            }
        } catch (DuplicateAnnotationException e) {
            throw new DuplicateAnnotationException(e.getMessage() + " for the same Query " + partition.toString());
        }
        if (partitionId == null) {
            this.partitionId = UUID.randomUUID().toString();
        }
        elementId = executionPlanContext.getElementIdGenerator().createNewId();
        this.partition = partition;
        this.streamDefinitionMap = streamDefinitionMap;
        this.streamJunctionMap = streamJunctionMap;
    }

    public QueryRuntime addQuery(QueryRuntime metaQueryRuntime) {
        Query query = metaQueryRuntime.getQuery();

        if (query.getOutputStream() instanceof InsertIntoStream && metaQueryRuntime.getOutputCallback() instanceof InsertIntoStreamCallback) {
            InsertIntoStreamCallback insertIntoStreamCallback = (InsertIntoStreamCallback) metaQueryRuntime.getOutputCallback();
            StreamDefinition streamDefinition = insertIntoStreamCallback.getOutputStreamDefinition();
            String id = streamDefinition.getId();

            if (((InsertIntoStream) query.getOutputStream()).isInnerStream()) {
                metaQueryRuntime.setToLocalStream(true);
                localStreamDefinitionMap.putIfAbsent(id, streamDefinition);
                DefinitionParserHelper.validateOutputStream(streamDefinition, localStreamDefinitionMap.get(id));

                StreamJunction outputStreamJunction = localStreamJunctionMap.get(id);

                if (outputStreamJunction == null) {
                    outputStreamJunction = new StreamJunction(streamDefinition,
                            executionPlanContext.getExecutorService(),
                            executionPlanContext.getSiddhiContext().getEventBufferSize(), executionPlanContext);
                    localStreamJunctionMap.putIfAbsent(id, outputStreamJunction);
                }
                insertIntoStreamCallback.init(localStreamJunctionMap.get(id));
            } else {
                streamDefinitionMap.putIfAbsent(id, streamDefinition);
                DefinitionParserHelper.validateOutputStream(streamDefinition, streamDefinitionMap.get(id));
                StreamJunction outputStreamJunction = streamJunctionMap.get(id);

                if (outputStreamJunction == null) {
                    outputStreamJunction = new StreamJunction(streamDefinition,
                            executionPlanContext.getExecutorService(),
                            executionPlanContext.getSiddhiContext().getEventBufferSize(), executionPlanContext);
                    streamJunctionMap.putIfAbsent(id, outputStreamJunction);
                }
                insertIntoStreamCallback.init(streamJunctionMap.get(id));
            }
        }
        metaQueryRuntimeMap.put(metaQueryRuntime.getQueryId(), metaQueryRuntime);

        return metaQueryRuntime;
    }

    public void addPartitionReceiver(QueryRuntime queryRuntime, List<VariableExpressionExecutor> executors, MetaStateEvent metaEvent) {
        Query query = queryRuntime.getQuery();
        List<List<PartitionExecutor>> partitionExecutors = new StreamPartitioner(query.getInputStream(), partition, metaEvent,
                executors, executionPlanContext).getPartitionExecutorLists();
        if (queryRuntime.getStreamRuntime() instanceof SingleStreamRuntime) {
            SingleInputStream singleInputStream = (SingleInputStream) query.getInputStream();
            addPartitionReceiver(singleInputStream.getStreamId(), singleInputStream.isInnerStream(), metaEvent.getMetaStreamEvent(0), partitionExecutors.get(0));
        } else if (queryRuntime.getStreamRuntime() instanceof JoinStreamRuntime) {
            SingleInputStream leftSingleInputStream = (SingleInputStream) ((JoinInputStream) query.getInputStream()).getLeftInputStream();
            addPartitionReceiver(leftSingleInputStream.getStreamId(), leftSingleInputStream.isInnerStream(), metaEvent.getMetaStreamEvent(0), partitionExecutors.get(0));
            SingleInputStream rightSingleInputStream = (SingleInputStream) ((JoinInputStream) query.getInputStream()).getRightInputStream();
            addPartitionReceiver(rightSingleInputStream.getStreamId(), rightSingleInputStream.isInnerStream(), metaEvent.getMetaStreamEvent(1), partitionExecutors.get(1));
        } else if (queryRuntime.getStreamRuntime() instanceof StateStreamRuntime) {
            StateElement stateElement = ((StateInputStream) query.getInputStream()).getStateElement();
            addPartitionReceiverForStateElement(stateElement, metaEvent, partitionExecutors, 0);
        }
    }

    private int addPartitionReceiverForStateElement(StateElement stateElement, MetaStateEvent metaEvent, List<List<PartitionExecutor>> partitionExecutors, int executorIndex) {
        if (stateElement instanceof EveryStateElement) {
            return addPartitionReceiverForStateElement(((EveryStateElement) stateElement).getStateElement(), metaEvent, partitionExecutors, executorIndex);
        } else if (stateElement instanceof NextStateElement) {
            executorIndex = addPartitionReceiverForStateElement(((NextStateElement) stateElement).getStateElement(), metaEvent, partitionExecutors, executorIndex);
            return addPartitionReceiverForStateElement(((NextStateElement) stateElement).getNextStateElement(), metaEvent, partitionExecutors, executorIndex);
        } else if (stateElement instanceof CountStateElement) {
            return addPartitionReceiverForStateElement(((CountStateElement) stateElement).getStreamStateElement(), metaEvent, partitionExecutors, executorIndex);
        } else if (stateElement instanceof LogicalStateElement) {
            executorIndex = addPartitionReceiverForStateElement(((LogicalStateElement) stateElement).getStreamStateElement1(), metaEvent, partitionExecutors, executorIndex);
            return addPartitionReceiverForStateElement(((LogicalStateElement) stateElement).getStreamStateElement2(), metaEvent, partitionExecutors, executorIndex);
        } else {  //if stateElement is an instanceof StreamStateElement
            SingleInputStream singleInputStream = ((StreamStateElement) stateElement).getBasicSingleInputStream();
            addPartitionReceiver(singleInputStream.getStreamId(), singleInputStream.isInnerStream(), metaEvent.getMetaStreamEvent(executorIndex), partitionExecutors.get(executorIndex));
            return ++executorIndex;
        }
    }

    private void addPartitionReceiver(String streamId, boolean isInnerStream, MetaStreamEvent metaStreamEvent, List<PartitionExecutor> partitionExecutors) {
        if (!partitionStreamReceivers.containsKey(streamId) && !isInnerStream && !metaStreamEvent.isTableEvent()) {
            PartitionStreamReceiver partitionStreamReceiver = new PartitionStreamReceiver(executionPlanContext, metaStreamEvent,
                    (StreamDefinition) streamDefinitionMap.get(streamId), partitionExecutors, this);
            partitionStreamReceivers.put(partitionStreamReceiver.getStreamId(), partitionStreamReceiver);
            streamJunctionMap.get(partitionStreamReceiver.getStreamId()).subscribe(partitionStreamReceiver);
        }
    }


    /**
     * clone all the queries of the partition for a given partition key if they are not available
     *
     * @param key partition key
     */
    public void cloneIfNotExist(String key) {
        PartitionInstanceRuntime partitionInstance = this.getPartitionInstanceRuntime(key);
        if (partitionInstance == null) {
            clonePartition(key);
        }
    }

    private synchronized void clonePartition(String key) {
        PartitionInstanceRuntime partitionInstance = this.getPartitionInstanceRuntime(key);

        if (partitionInstance == null) {
            List<QueryRuntime> queryRuntimeList = new ArrayList<QueryRuntime>();
            List<QueryRuntime> partitionedQueryRuntimeList = new CopyOnWriteArrayList<QueryRuntime>();

            for (QueryRuntime queryRuntime : metaQueryRuntimeMap.values()) {

                QueryRuntime clonedQueryRuntime = queryRuntime.clone(key, localStreamJunctionMap);
                queryRuntimeList.add(clonedQueryRuntime);

                if (queryRuntime.isFromLocalStream()) {
                    for (int i = 0; i < clonedQueryRuntime.getStreamRuntime().getSingleStreamRuntimes().size(); i++) {
                        String streamId = queryRuntime.getStreamRuntime().getSingleStreamRuntimes().get(i).getProcessStreamReceiver().getStreamId();
                        StreamDefinition streamDefinition;
                        if (streamId.startsWith("#")) {
                            streamDefinition = (StreamDefinition) localStreamDefinitionMap.get(streamId);
                        } else {
                            streamDefinition = (StreamDefinition) streamDefinitionMap.get(streamId);
                        }
                        StreamJunction streamJunction = localStreamJunctionMap.get(streamId + key);
                        if (streamJunction == null) {
                            streamJunction = new StreamJunction(streamDefinition, executionPlanContext.getExecutorService(),
                                    executionPlanContext.getSiddhiContext().getEventBufferSize(), executionPlanContext);
                            localStreamJunctionMap.put(streamId + key, streamJunction);
                        }
                        streamJunction.subscribe(clonedQueryRuntime.getStreamRuntime().getSingleStreamRuntimes().get(i).getProcessStreamReceiver());
                    }
                } else {
                    partitionedQueryRuntimeList.add(clonedQueryRuntime);
                }
            }
            addPartitionInstance(new PartitionInstanceRuntime(key, queryRuntimeList));
            updatePartitionStreamReceivers(key, partitionedQueryRuntimeList);

        }

    }

    private void updatePartitionStreamReceivers(String key, List<QueryRuntime> partitionedQueryRuntimeList) {
        for (PartitionStreamReceiver partitionStreamReceiver : partitionStreamReceivers.values()) {
            partitionStreamReceiver.addStreamJunction(key, partitionedQueryRuntimeList);
        }
    }

    public void addPartitionInstance(PartitionInstanceRuntime partitionInstanceRuntime) {
        partitionInstanceRuntimeList.add(partitionInstanceRuntime);
    }

    public PartitionInstanceRuntime getPartitionInstanceRuntime(String key) {
        for (PartitionInstanceRuntime partitionInstanceRuntime : partitionInstanceRuntimeList) {
            if (key.equals(partitionInstanceRuntime.getKey())) {
                return partitionInstanceRuntime;
            }
        }
        return null;
    }

    public void addStreamJunction(String key, StreamJunction streamJunction) {
        localStreamJunctionMap.put(key, streamJunction);
    }

    public String getPartitionId() {
        return partitionId;
    }

    public ConcurrentMap<String, AbstractDefinition> getLocalStreamDefinitionMap() {
        return localStreamDefinitionMap;
    }

    public ConcurrentMap<String, StreamJunction> getLocalStreamJunctionMap() {
        return localStreamJunctionMap;
    }

    @Override
    public Object[] currentState() {
        List<String> partitionKeys = new ArrayList<String>();
        for (PartitionInstanceRuntime partitionInstanceRuntime : partitionInstanceRuntimeList) {
            partitionKeys.add(partitionInstanceRuntime.getKey());
        }
        return new Object[]{partitionKeys};
    }

    @Override
    public void restoreState(Object[] state) {
        List<String> partitionKeys = (List<String>) state[0];
        for (String key : partitionKeys) {
            clonePartition(key);
        }
    }

    @Override
    public String getElementId() {
        return elementId;
    }
}
