package Leader_Selection;

import Lock.lock.TestMainClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * LeaderElection
 * Created Date: 2014-11-13
 */

/**
 * Master选举
 *
 * 比如我在zookeeper服务器端有一个znode叫/Configuration，
 * 那么集群中每一个机器启动的时候都去这个节点下创建一个EPHEMERAL类型的节点，
 *
 * 比如server1创建/Configuration /Server1，server2创建/Configuration /Server1，
 * 然后Server1和Server2都watch /Configuration 这个父节点，
 * 那么也就是这个父节点下数据或者子节点变化都会通知对该节点进行watch的客户端。
 * 因为EPHEMERAL类型节点有一个很重要的特性，就是客户端和服务器端连接断掉或者session过期就会使节点消失，
 * 那么在某一个机器挂掉或者断链的时候，其对应的节点就会消失，
 * 然后集群中所有对/Configuration进行watch的客户端都会收到通知，然后取得最新列表即可。
 */
public class LeaderElection extends TestMainClient {
    public static final Logger logger = Logger.getLogger(LeaderElection.class);

    public LeaderElection(String connectString, String root) {
        super(connectString);
        this.root = root;
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                logger.error(e);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    void findLeader() throws InterruptedException, UnknownHostException, KeeperException {
        byte[] leader = null;
        try {
            leader = zk.getData(root + "/leader", true, null);
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NoNodeException) {
                logger.error(e);
            } else {
                throw e;
            }
        }
        if (leader != null) {
            following();
        } else {
            String newLeader = null;
            byte[] localhost = InetAddress.getLocalHost().getAddress();
            try {
                newLeader = zk.create(root + "/leader", localhost, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NodeExistsException) {
                    logger.error(e);
                } else {
                    throw e;
                }
            }
            if (newLeader != null) {
                leading();
            } else {
                mutex.wait();
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getPath().equals(root + "/leader") && event.getType() == Event.EventType.NodeCreated) {
            System.out.println("得到通知");
            super.process(event);
            following();
        }
    }

    void leading() {
        System.out.println("成为领导者");
    }

    void following() {
        System.out.println("成为组成员");
    }

    public static void main(String[] args) {
        String connectString = "localhost:2181";

        LeaderElection le = new LeaderElection(connectString, "/GroupMembers");
        try {
            le.findLeader();
        } catch (Exception e) {
            logger.error(e);
        }
    }
}