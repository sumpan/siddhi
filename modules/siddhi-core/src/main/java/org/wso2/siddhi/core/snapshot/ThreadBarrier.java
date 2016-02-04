package org.wso2.siddhi.core.snapshot;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class ThreadBarrier {
    private boolean open = true;
    
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition threadSwitchCondition  = lock.newCondition();
    /*典型的独占锁与condition*/
    /**/
    public void pass() {
        lock.lock();/*重入锁，open如果是false，那么当前线程就会阻塞住，pass本身就是阻塞的，进来之后，还要看条件，*/
        if(!open){
//            threadSwitchCondition.awaitUninterruptibly();
            try {
                threadSwitchCondition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock.unlock();
     }
    
    public void open(){
        lock.lock();
        if(!open){
            open = true;
            threadSwitchCondition.signalAll(); 
        }
        lock.unlock();
    }

    public void close() {
        lock.lock();
        open = false;
        lock.unlock();

    }

}
