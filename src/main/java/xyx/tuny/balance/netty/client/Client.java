package xyx.tuny.balance.netty.client;

public interface Client {
    // 连接服务器
    public void connect() throws Exception;
    // 断开服务器
    public void disConnect() throws Exception;
}
