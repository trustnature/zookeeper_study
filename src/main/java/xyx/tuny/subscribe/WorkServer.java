package xyx.tuny.subscribe;

import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;

public class WorkServer {

    private CuratorFramework client;
    private String configPath;
    private String serversPath;
    private ServerData serverData;
    private ServerConfig serverConfig;
    private NodeCacheListener dataListener;
    private NodeCache cache;

    public WorkServer(String configPath, String serversPath, ServerData serverData, CuratorFramework client, ServerConfig initConfig) {
        this.client = client;
        this.configPath = configPath;
        this.serversPath = serversPath;
        this.serverData = serverData;
        this.serverConfig = initConfig;

        this.cache = new NodeCache(client,configPath,false);
        this.dataListener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                String retJson = new String(cache.getCurrentData().getData());
                ServerConfig serverConfigLocal = (ServerConfig) JSON.parseObject(retJson, ServerConfig.class);
                updateConfig(serverConfigLocal);
                System.out.println("new Work server config is:" + serverConfig.toString());
            }
        };


    }

    public void start() {
        System.out.println("work server start...");
        client.start();
        initRunning();
    }

    public void stop() {
        System.out.println("work server stop...");
        cache.getListenable().removeListener(dataListener);
    }

    private void initRunning() {
        registMe();
        cache.getListenable().addListener(dataListener);
    }

    private void registMe() {
        String mePath = serversPath.concat("/").concat(serverData.getAddress());
        System.out.println("registe workserver:" + mePath);
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(mePath,JSON.toJSONString(serverData).getBytes());
        } catch (Exception e) {
            try {
                client.create().withMode(CreateMode.PERSISTENT).forPath(serversPath);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    private void updateConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }
}