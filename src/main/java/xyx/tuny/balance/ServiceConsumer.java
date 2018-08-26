package xyx.tuny.balance;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ServiceConsumer {
    private List<String> serverList=new ArrayList<String>();

    private String serviceName="service-A";

    public void init(){
        String zkServerList="172.19.100.29:2181";
        final String servicePath="/servers/"+serviceName;

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();
        PathChildrenCache childCache = new PathChildrenCache(client, servicePath, true);
        try {
            childCache.start(StartMode.POST_INITIALIZED_EVENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathChildrenCacheListener childListener = new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client,
                                   PathChildrenCacheEvent event) throws Exception {
                System.out.println("work server list changed :" + event.getType());
                switch(event.getType()){
                    case CHILD_ADDED:
                        serverList = client.getChildren().forPath(servicePath);
                        System.out.println("work server list changed add, new list is " + serverList);
                        break;
                    case CHILD_REMOVED:
                        serverList = client.getChildren().forPath(servicePath);
                        System.out.println("work server list changed remove, new list is " + serverList);
                        break;
                    default:
                        break;
                }
                consume();
            }
        };
        childCache.getListenable().addListener(childListener);

        try {
            if(null == client.checkExists().forPath(servicePath)){
                System.out.println("当前服务的地址"+serverList);
            }else{
                serverList = client.getChildren().forPath(servicePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //通过负责均衡算法，得到一台服务器进行调用
    public void consume(){
        int index = getRandomNum(0,1);
        System.out.println("调用" + serverList.get(index)+"提供的服务：" + serviceName);
    }

    public int getRandomNum(int min,int max){
        Random rdm = new Random();
        return rdm.nextInt(max-min+1)+min;
    }

    public static void main(String[] args)throws Exception {
        ServiceConsumer consumer = new ServiceConsumer();

        consumer.init();

        Thread.sleep(60*60);
    }
}
