package xyx.tuny.master;

import com.alibaba.fastjson.JSON;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WorkServer {

    //客户端状态
    private volatile boolean running = false;

    private CuratorFramework client;

    //主节点路径
    public static final String MASTER_PATH = "/master";

    //监听(用于监听主节点删除事件)
    private NodeCacheListener dataListener;

    //服务器基本信息
    private RunningData serverData;
    //主节点基本信息
    private RunningData masterData;
    private NodeCache cache;
    //调度器
    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);
    //延迟时间5s
    private int delayTime = 5;

    public WorkServer(RunningData runningData) {
        this.serverData = runningData;
    }

    //启动
    public void start() throws Exception {
        if (running) {
            throw new Exception("server has startup....");
        }
        running = true;
        client.start();
        this.cache = new NodeCache(client, MASTER_PATH, false);

        this.dataListener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                //System.out.println("--nodes changed and previous server is " + masterData.getName() + "--");
                Stat stat = client.checkExists().forPath(MASTER_PATH);
                if(null == stat){
                    //takeMaster();
                    if (masterData != null && masterData.getName().equals(serverData.getName())) {//若之前master为本机,则立即抢主,否则延迟5秒抢主(防止小故障引起的抢主可能导致的网络数据风暴)
                        System.out.println("--now server " + serverData.getName() + " has privilege to take master");
                        takeMaster();
                    } else {
                        delayExector.schedule(new Runnable() {
                            @Override
                            public void run() {
                                takeMaster();
                            }
                        }, delayTime, TimeUnit.SECONDS);
                    }
                }

            }
        };
        this.cache.getListenable().addListener(dataListener);
        try {
            cache.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        takeMaster();
    }

    //停止
    public void stop() throws Exception {
        if (!running) {
            throw new Exception("server has stopped.....");
        }
        running = false;
        delayExector.shutdown();
        cache.getListenable().removeListener(dataListener);
        releaseMaster();
    }

    //抢注主节点
    private void takeMaster() {
        if (!running) return;
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(MASTER_PATH, JSON.toJSONString(serverData).getBytes());
            masterData = serverData;
            System.out.println("--" + serverData.getName() + " now is Master--");
            delayExector.schedule(new Runnable() {//测试抢主用,每5s释放一次主节点
                @Override
                public void run() {
                    if (checkMaster()) {
                        releaseMaster();
                    }
                }
            }, 5, TimeUnit.SECONDS);
        } catch (Exception e){//节点已存在
            String retJson = null;
            try {
                retJson = new String(client.getData().forPath(MASTER_PATH));
                RunningData runningData = null;
                runningData = (RunningData)JSON.parseObject(retJson, RunningData.class);
                if(runningData == null){//读取主节点时,主节点被释放
                    takeMaster();
                }else{
                    masterData = runningData;
                }
            } catch (Exception e1) {

            }
        }

    }

    //释放主节点
    private void releaseMaster() {
        if (checkMaster()) {
            try {
                client.delete().forPath(MASTER_PATH);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //检验自己是否是主节点
    private boolean checkMaster() {
        try {
            String retJson = new String(cache.getCurrentData().getData());
            RunningData runningData = (RunningData) JSON.parseObject(retJson, RunningData.class);
            masterData = runningData;
            if (masterData.getName().equals(serverData.getName())) {
                return true;
            }
            return false;

        } catch (ZkNoNodeException e) {//节点不存在
            return false;
        } catch (ZkInterruptedException e) {//网络中断
            return checkMaster();
        } catch (Exception e) {//其它
            return false;
        }
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }

    public CuratorFramework getClient() {
        return client;
    }
}