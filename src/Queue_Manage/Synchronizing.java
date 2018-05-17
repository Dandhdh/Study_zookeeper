package Queue_Manage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import Lock.lock.TestMainClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Zookeeper 可以处理两种类型的队列：
 * ① 当一个队列的成员都聚齐时，这个队列才可用，否则一直等待所有成员到达，这种是同步队列。
 * ② 队列按照 FIFO 方式进行入队和出队操作，例如实现生产者和消费者模型。
 */

/**
 * 这里是-----同步队列
 *
 * 同步队列用 Zookeeper 实现的实现思路如下：
 *
 * 创建一个父目录 /synchronizing，
 * 每个成员都监控标志（Set Watch）位目录 /synchronizing/start 是否存在，
 * 然后每个成员都加入这个队列，加入队列的方式就是创建 /synchronizing/member_i 的临时目录节点，
 * 然后每个成员获取 / synchronizing 目录的所有目录节点，也就是 member_i。
 * 判断 i 的值是否已经是成员的个数，如果小于成员个数等待 /synchronizing/start 的出现，
 * 如果已经相等就创建 /synchronizing/start。
 *
 */
public class Synchronizing extends TestMainClient {
    int size;
    String name;
    public static final Logger logger = Logger.getLogger(Synchronizing.class);

    /**
     * 构造函数
     *
     * @param connectString 服务器连接
     * @param root          根目录
     * @param size          队列大小
     */
    Synchronizing(String connectString, String root, int size) {
        super(connectString);
        this.root = root;
        this.size = size;

        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                logger.error(e);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
        try {
            name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
        } catch (UnknownHostException e) {
            logger.error(e);
        }

    }

    /**
     * 加入队列
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */

    void addQueue() throws KeeperException, InterruptedException {
        zk.exists(root + "/start", true);
        zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        synchronized (mutex) {
            List<String> list = zk.getChildren(root, false);
            if (list.size() < size) {
                mutex.wait();
            } else {
                zk.create(root + "/start", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getPath().equals(root + "/start") && event.getType() == Event.EventType.NodeCreated) {
            System.out.println("得到通知");
            super.process(event);
            doAction();
        }
    }

    /**
     * 执行其他任务
     */
    private void doAction() {
        System.out.println("同步队列已经得到同步，可以开始执行后面的任务了");
    }

    public static void main(String args[]) {
        //启动Server
        String connectString = "localhost:2181";
        int size = 1;
        Synchronizing b = new Synchronizing(connectString, "/synchronizing", size);
        try {
            b.addQueue();
        } catch (KeeperException e) {
            logger.error(e);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }
}