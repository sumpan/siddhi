/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.siddhi.core.stream;

import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.query.processor.handler.HandlerProcessor;
import org.wso2.siddhi.core.tracer.EventMonitorService;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class StreamJunction {
    /*本来以为Junction 里面有很多队列，接受用户数据发射进来，其实不是，Junction相当于是个代理，相当于一个集市
    * 将数据流的交易管理起来，可以理解成单生产者，多消费者模型，Junction里面就是多个消费者*/
    private List<StreamReceiver> streamReceivers = new CopyOnWriteArrayList<StreamReceiver>();
    /*这个相当于一个线程安全的arraylist*/
    private String streamId;
    private EventMonitorService eventMonitorService;

    public StreamJunction(String streamId, EventMonitorService eventMonitorService) {
        this.streamId = streamId;
        this.eventMonitorService = eventMonitorService;
    }

    public void send(StreamEvent allEvents) {
        if (eventMonitorService.isEnableTrace()) {
            eventMonitorService.trace(allEvents, " on Event Stream");
        }
        if (eventMonitorService.isEnableStats()) {
            eventMonitorService.calculateStats(allEvents);
        }
        /*时间监听服务，是一个多线程共享的对象，目的就是为了追踪和统计tps等，尤其这个trace服务，
        * 相当于把全部事件给你打开了，这个在超大规模的流处理场景中，肯定是不行的*/
        for (StreamReceiver handlerProcessor : streamReceivers) {
            handlerProcessor.receive(allEvents);
        }/*发射数据，其实就是让那些消费者来消费*/
    }

    public synchronized void addEventFlow(StreamReceiver streamReceiver) {
        //in reverse order to execute the later states first to overcome to dependencies of count states
        streamReceivers.add(0, streamReceiver);
    }/*新加的消费者反而最早消费到数据，*/

    public synchronized void removeEventFlow(HandlerProcessor queryStreamProcessor) {
        streamReceivers.remove(queryStreamProcessor);
    }

    public String getStreamId() {
        return streamId;
    }
}
