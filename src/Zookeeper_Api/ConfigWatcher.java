package Zookeeper_Api;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * 通过使用 ActiveKeyValueStore的read()方法 实现配置的更新
 */

/**
 * 当ConfigUpdater更新znode时，ZooKeeper产生一个类型为EventType.NodeDataChanged的 事件，从而触发观察。
 * ConfigWatcher在它的process()方法中对这个事件做出反应，读取并显示配置的最新版本。
 *
 * 由于观察仅发送单次信号，因此每次我们调用ActiveKeyValueStore的read()方法时，
 * 都将一个新的观察告知ZooKeeper来确保我们可以看到将来的更新。
 *
 * 但是，我们还是不能保证接收到每一个更新，因为在收到观察事件通知与下一次读之间，
 * znode可能已经被更新过，而且可能是很多次，由于客户端在这段 时间没有注册任何观察，
 * 因此不会收到通知。对于示例中的配置服务，这不是问题，因为客户端只关心属性的最新值，
 * 最新值优先于之前的值。但是，一般情况下， 这个潜在的问题是不容忽视的。
 *
 */
public class ConfigWatcher implements Watcher{
    private ActiveKeyValueStore store;

    @Override
    public void process(WatchedEvent event) {
        if(event.getType()==EventType.NodeDataChanged){
            try{
                dispalyConfig();
            }catch(InterruptedException e){
                System.err.println("Interrupted. exiting. ");
                Thread.currentThread().interrupt();
            }catch(KeeperException e){
                System.out.printf("KeeperException?s. Exiting.\n", e);
            }
            
        }
        
    }
    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        store=new ActiveKeyValueStore();
        store.connect(hosts);
    }
    public void dispalyConfig() throws KeeperException, InterruptedException{
        String value=store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s\n",ConfigUpdater.PATH,value);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigWatcher configWatcher = new ConfigWatcher("127.0.0.1");
        configWatcher.dispalyConfig();
        //stay alive until process is killed or Thread is interrupted
        Thread.sleep(Long.MAX_VALUE);
    }
}