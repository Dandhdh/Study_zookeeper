package Zookeeper_Api;

import java.nio.charset.Charset;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ActiveKeyValueStore extends ConnectionWatcher {

    private static final Charset CHARSET=Charset.forName("UTF-8");

    /**
     * write()方法的任务是将一个关键字(路径)及其值写到ZooKeeper。
     * 它隐藏了创建一个新的znode和用一个新值更新现有znode之间的区别，
     * 而是使用exists操作来检测znode是否存在，然后再执行相应的操作。
     *
     * @param path
     * @param value
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void write(String path,String value) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if(stat==null){
            zk.create(path, value.getBytes(CHARSET),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }else{
            zk.setData(path, value.getBytes(CHARSET),-1);
        }
    }
    public String read(String path,Watcher watch) throws KeeperException, InterruptedException{
        /**
         * 三个参数
         * （1）路径
         * （2）一个观察对象
         * （3）一个Stat对象
         *
         * Stat对象由getData()方法返回的值填充，用来将信息回传给调用者。
         * 通过这个方法，调用者可以获得一个znode的数据和元数据，
         * 但在这个例子中，由于我们对元数据不感兴趣，因此将Stat参数设为null。
         */
        byte[] data = zk.getData(path, watch, null);
        return new String(data,CHARSET);
        
    }
    
}