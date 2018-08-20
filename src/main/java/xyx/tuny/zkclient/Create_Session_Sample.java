package xyx.tuny.zkclient;
import java.io.IOException;
import org.I0Itec.zkclient.ZkClient;

// 使用ZkClient来创建一个ZooKeeper客户端
public class Create_Session_Sample {
    public static void main(String[] args) throws IOException, InterruptedException {
    	ZkClient zkClient = new ZkClient("127.0.0.1:2181", 5000);
    	System.out.println("ZooKeeper session established.");
    }
}