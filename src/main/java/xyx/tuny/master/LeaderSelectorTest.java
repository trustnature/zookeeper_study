package xyx.tuny.master;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * master选举
 */
public class LeaderSelectorTest {

    //启动的服务个数
    private static final int  CLIENT_QTY = 10;

    public static void main(String[] args) throws Exception{

        List<CuratorFramework> clients = new ArrayList<CuratorFramework>();
        List<WorkServer>  workServers = new ArrayList<WorkServer>();

        try{
            for ( int i = 0; i < CLIENT_QTY; ++i ){
                //创建client
                CuratorFramework client = CuratorFrameworkFactory.builder()
                        .connectString("127.0.0.1:2181")
                        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                        .sessionTimeoutMs(5000)
                        .build();
                clients.add(client);
                //创建serverData
                RunningData runningData = new RunningData();
                runningData.setCid(Long.valueOf(i));
                runningData.setName("Client #" + i);
                //创建服务
                WorkServer workServer = new WorkServer(runningData);
                workServer.setClient(client);

                workServers.add(workServer);
                workServer.start();
            }

            System.out.println("敲回车键退出！\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        }finally{
            System.out.println("Shutting down...");

            for ( WorkServer workServer : workServers ){
                try {
                    workServer.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for ( CuratorFramework client : clients ){
                try {
                    client.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
