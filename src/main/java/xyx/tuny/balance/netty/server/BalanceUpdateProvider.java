package xyx.tuny.balance.netty.server;

public interface BalanceUpdateProvider {
    // 增加负载
    public boolean addBalance(Integer step);

    // 减少负载
    public boolean reduceBalance(Integer step);
}
