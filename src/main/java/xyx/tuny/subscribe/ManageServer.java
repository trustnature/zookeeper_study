package xyx.tuny.subscribe;

import java.util.List;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

public class ManageServer {

    private String serversPath;
    private String commandPath;
    private String configPath;
    private CuratorFramework client;
    private ServerConfig config;
    private PathChildrenCacheListener childListener;
    private NodeCacheListener dataListener;
    private List<String> workServerList;
    private NodeCache cache;
    private PathChildrenCache childCache;

    public ManageServer(final String serversPath, String commandPath,
                        String configPath, CuratorFramework client, ServerConfig config) {
        this.serversPath = serversPath;
        this.commandPath = commandPath;
        this.client = client;
        this.config = config;
        this.configPath = configPath;

    }

    private void initRunning() {
        this.client.start();

        this.childCache = new PathChildrenCache(client, serversPath, true);
        try {
            childCache.start(StartMode.POST_INITIALIZED_EVENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.childListener = new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client,
                                   PathChildrenCacheEvent event) throws Exception {
                switch(event.getType()){
                    case CHILD_ADDED:
                        workServerList = client.getChildren().forPath(serversPath);
                        System.out.println("work server list changed add, new list is ");
                        execList();
                        break;
                    case CHILD_REMOVED:
                        workServerList = client.getChildren().forPath(serversPath);
                        System.out.println("work server list changed remove, new list is ");
                        execList();
                        break;
                }
            }
        };
        this.childCache.getListenable().addListener(childListener);

        this.cache = new NodeCache(client,commandPath,false);

        this.dataListener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                String cmd = new String(cache.getCurrentData().getData());
                System.out.println("cmd:" + cmd);
                exeCmd(cmd);
            }
        };
        this.cache.getListenable().addListener(dataListener);
        try {
            cache.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /*
     * 1: list 2: create 3: modify
     */
    private void exeCmd(String cmdType) {
        if ("list".equals(cmdType)) {
            execList();

        } else if ("create".equals(cmdType)) {
            execCreate();
        } else if ("modify".equals(cmdType)) {
            execModify();
        } else {
            System.out.println("error command!" + cmdType);
        }
    }

    private void execList() {
        System.out.println(workServerList.toString());
    }

    private void execCreate() {
            try {
                if (client.checkExists().forPath(configPath) == null) {
                    client.create().withMode(CreateMode.PERSISTENT).forPath(configPath, JSON.toJSONString(config).getBytes());
                    client.setData().forPath(configPath, JSON.toJSONString(config).getBytes());
                }
            } catch (Exception e) {
                String parentDir = configPath.substring(0, configPath.lastIndexOf('/'));
                try {
                    client.create().withMode(CreateMode.PERSISTENT).forPath(parentDir);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                execCreate();
            }

    }

    private void execModify() {
        config.setDbUser(config.getDbUser() + "_modify");

        try {
            client.setData().forPath(configPath, JSON.toJSONString(config).getBytes());
        } catch (Exception e) {
            execCreate();
        }
    }

    public void start() {
        initRunning();
    }

    public void stop() {
        cache.getListenable().removeListener(dataListener);
        childCache.getListenable().removeListener(childListener);
    }
}