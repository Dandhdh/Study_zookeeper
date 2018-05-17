package Zookeeper_Api;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 在ZooKeeper中新建表示组的Znode
 */
public class CreateGroup implements Watcher {
    private static final int SESSION_TIMEOUT=5000;
    
    private ZooKeeper zk;
    private CountDownLatch connectedSignal=new CountDownLatch(1);
    @Override
    public void process(WatchedEvent event) {
        if(event.getState()==Event.KeeperState.SyncConnected){
            connectedSignal.countDown();
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect(args[0]);
        createGroup.create(args[1]);
        createGroup.close();
    }

    private void close() throws InterruptedException {
        zk.close();
    }

    private void create(String groupName) throws KeeperException, InterruptedException {
        String path="/"+groupName;
        if(zk.exists(path, false)== null){
            zk.create(path, null/*data*/, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println("Created:"+path);
    } 

    private void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }
}