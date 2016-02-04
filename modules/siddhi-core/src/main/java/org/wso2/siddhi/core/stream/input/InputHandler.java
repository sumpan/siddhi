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
package org.wso2.siddhi.core.stream.input;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.ListEvent;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.snapshot.ThreadBarrier;
import org.wso2.siddhi.core.stream.StreamJunction;

public class InputHandler {
    private String streamId;
    private StreamJunction streamJunction;
    private final ThreadBarrier threadBarrier;

    public InputHandler(String streamId, StreamJunction streamJunction,SiddhiContext siddhiContext) {
        this.streamId = streamId;
        this.streamJunction = streamJunction;
        this.threadBarrier =siddhiContext.getThreadBarrier();
    }/*所有的InputHandler对象共享同一个ThreadBarrier*/

    public void send(Object[] data) throws InterruptedException {
        StreamEvent event = new InEvent(streamId, System.currentTimeMillis(), data);
        /*内部事件*/
        threadBarrier.pass();/*首先尝试去拿那个独占锁，拿到之后，如果barrier open的话就释放掉，然后往下执行
        即你要往下执行需要过两关，首先是拿锁，然后还有一个控制位，控制位状态不改变，你只能阻塞等，而不能执行数据发送任务*/
        streamJunction.send(event);
    }

    public void send(long timeStamp, Object[] data) throws InterruptedException {
        StreamEvent event = new InEvent(streamId, timeStamp, data);
        threadBarrier.pass();
        streamJunction.send(event);
    }

    public void send(StreamEvent event) throws InterruptedException {
        threadBarrier.pass();
        streamJunction.send(event);
    }
    public void send(ListEvent listEvent) throws InterruptedException {
        threadBarrier.pass();
        streamJunction.send(listEvent);
    }

    public String getStreamId() {
        return streamId;
    }
}
