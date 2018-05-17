package Zookeeper_Api;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

/**
 * 通过使用 ActiveKeyValueStore的write()方法 实现配置的更新
 */
public class ConfigUpdater {
    
    public static final String  PATH="/config";
    
    private ActiveKeyValueStore store;
    private Random random=new Random();
    
    public ConfigUpdater(String hosts) throws IOException, InterruptedException {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }
    public void run() throws InterruptedException, KeeperException{
        //设置为每个一段时间更新
        while(true){
            String value=random.nextInt(100)+"";
            store.write(PATH, value);
            System.out.printf("Set %s to %s\n",PATH,value);
            TimeUnit.SECONDS.sleep(random.nextInt(100));
            
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigUpdater configUpdater = new ConfigUpdater("127.0.0.1");
        configUpdater.run();
    }
}