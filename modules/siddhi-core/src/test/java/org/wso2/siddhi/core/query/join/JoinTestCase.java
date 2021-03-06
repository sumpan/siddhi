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
package org.wso2.siddhi.core.query.join;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class JoinTestCase {
    private static final Logger log = Logger.getLogger(JoinTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void joinTest1() throws InterruptedException {
        log.info("Join test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) join twitterStream#window.time(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(500);
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(2000);
        Assert.assertEquals(2, inEventCount);
        Assert.assertEquals(2, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void joinTest2() throws InterruptedException {
        log.info("Join test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) as a join twitterStream#window.time(1 sec) as b " +
                "on a.symbol== b.company " +
                "select a.symbol as symbol, b.tweet, a.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(500);
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(2000);
        Assert.assertEquals(2, inEventCount);
        Assert.assertEquals(2, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void joinTest3() throws InterruptedException {
        log.info("Join test3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(500 milliseconds) as a join cseEventStream#window.time(500 milliseconds) as b " +
                "on a.symbol== b.symbol " +
                "select a.symbol as symbol, a.price as priceA, b.price as priceB " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(2000);
        executionPlanRuntime.shutdown();
        Assert.assertEquals(2, inEventCount);
        Assert.assertEquals(2, removeEventCount);
        Assert.assertTrue(eventArrived);


    }

    @Test
    public void joinTest4() throws InterruptedException {
        log.info("Join test4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) join twitterStream#window.time(2 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "within 1 sec " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        org.junit.Assert.assertTrue("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0)));
                    }
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    for (Event event : removeEvents) {
                        org.junit.Assert.assertTrue("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0)));
                    }
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(1300);
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(3000);
        Assert.assertEquals(1, inEventCount);
        Assert.assertEquals(1, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void joinTest5() throws InterruptedException {
        log.info("Join test5");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(1) join twitterStream#window.length(1) " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void joinTest6() throws InterruptedException {
        log.info("Join test6");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, symbol string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream join twitterStream " +
                "select symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void joinTest7() throws InterruptedException {
        log.info("Join test7");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, symbol string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream as a join twitterStream as b " +
                "select a.symbol, twitterStream.tweet, a.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
    }

    @Test
    public void joinTest8() throws InterruptedException {
        log.info("Join test8");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(1) join twitterStream#window.length(1) " +
                "select cseEventStream.symbol as symbol, tweet, price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void joinTest9() throws InterruptedException {
        log.info("Join test9");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream join twitterStream " +
                "select count() as events, symbol " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(500);
        Assert.assertFalse(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void joinTest10() throws InterruptedException {
        log.info("Join test10");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream join twitterStream#window.length(1) " +
                "select count() as events, symbol " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                }
                if (removeEvents != null) {
                    for (Event event : removeEvents) {
                        removeEventCount++;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals("inEventCount", 2, inEventCount);
        Assert.assertEquals("removeEventCount", 0, removeEventCount);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void joinTest11() throws InterruptedException {
        log.info("Join test11");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream unidirectional join twitterStream#window.length(1) " +
                "select count() as events, symbol, tweet " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                }
                if (removeEvents != null) {
                    for (Event event : removeEvents) {
                        removeEventCount++;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
        cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals("inEventCount", 2, inEventCount);
        Assert.assertEquals("removeEventCount", 2, removeEventCount);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void joinTest12() throws InterruptedException {
        log.info("Join test12");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) join twitterStream#window.time(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
        executionPlanRuntime.start();
        cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
        twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
        Thread.sleep(100);
        Assert.assertEquals(1, inEventCount);
        Assert.assertEquals(0, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void joinTest13() throws InterruptedException {
        log.info("Join test13");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, symbol string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) join twitterStream#window.time(1 sec) " +
                "on cseEventStream.symbol== twitterStream.symbol " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }


}
