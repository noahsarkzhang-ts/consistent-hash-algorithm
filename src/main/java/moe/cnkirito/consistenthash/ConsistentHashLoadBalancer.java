package moe.cnkirito.consistenthash;


import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author daofeng.xjf
 * @date 2019/2/15
 */
public class ConsistentHashLoadBalancer implements LoadBalancer{

    /**
     *  使用的哈希算法
     */
    private HashStrategy hashStrategy = new FnvHashStrategy();

    /**
     *  虚拟结点的个数
     */
    private final static int VIRTUAL_NODE_SIZE = 10;

    /**
     *  服务器与虚拟结点的连接符
     */
    private final static String VIRTUAL_NODE_SUFFIX = "&&";

    /**
     * 根据请求匹配服务器
     * @param servers 服务器数组
     * @param invocation 封装了调用的请求
     * @return 返回匹配的值
     */
    @Override
    public Server select(List<Server> servers, Invocation invocation) {
        int invocationHashCode = hashStrategy.getHashCode(invocation.getHashKey());
        TreeMap<Integer, Server> ring = buildConsistentHashRing(servers);
        Server server = locate(ring, invocationHashCode);
        return server;
    }

    /**
     * 从TreeMap中找出高key最近的服务器，如果没有服务器匹配，
     * 则返回第一值，形成一个环
     * @param ring 存储服务器结点的哈希
     * @param invocationHashCode 封装了调用的请求
     * @return
     */
    private Server locate(TreeMap<Integer, Server> ring, int invocationHashCode) {
        // 向右找到第一个 key
        Map.Entry<Integer, Server> locateEntry = ring.ceilingEntry(invocationHashCode);
        if (locateEntry == null) {
            // 想象成一个环，超过尾部则取第一个 key
            locateEntry = ring.firstEntry();
        }
        return locateEntry.getValue();
    }

    /**
     * 构建一个带虚拟结点的哈希环，使用TreeMap结构，底层是红黑树，
     * @param servers 服务器数组
     * @return 返回服务器数组的哈希环
     */
    private TreeMap<Integer, Server> buildConsistentHashRing(List<Server> servers) {
        TreeMap<Integer, Server> virtualNodeRing = new TreeMap<>();
        for (Server server : servers) {
            for (int i = 0; i < VIRTUAL_NODE_SIZE; i++) {
                // 新增虚拟节点的方式如果有影响，也可以抽象出一个由物理节点扩展虚拟节点的类
                virtualNodeRing.put(hashStrategy.getHashCode(server.getUrl() + VIRTUAL_NODE_SUFFIX + i), server);
            }
        }
        return virtualNodeRing;
    }

}
