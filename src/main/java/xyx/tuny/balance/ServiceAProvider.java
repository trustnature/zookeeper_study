package xyx.tuny.balance;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;


public class ServiceAProvider {
    private String serviceName="service-A";

    public void init(){
        String rootPath="/servers";
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();
        String ip="192.168.1.23";
        try {
            if(null == client.checkExists().forPath(rootPath))
                client.create().withMode(CreateMode.PERSISTENT).forPath(rootPath);
            if(null == client.checkExists().forPath(rootPath+"/"+serviceName))
                client.create().withMode(CreateMode.PERSISTENT).forPath(rootPath+"/"+serviceName);
            if(null == client.checkExists().forPath(rootPath+"/"+serviceName+"/"+ip))
                client.create().withMode(CreateMode.PERSISTENT).forPath(rootPath+"/"+serviceName+"/"+ip);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("znode:"+rootPath+"/"+serviceName+"/"+ip+"创建完成");
    }

    //提供服务
    public void provide(){

    }

    public static void main(String[]args) throws Exception {
        ServiceAProvider service = new ServiceAProvider();
        service.init();

        Thread.sleep(1000*60*60*24);
    }
}
