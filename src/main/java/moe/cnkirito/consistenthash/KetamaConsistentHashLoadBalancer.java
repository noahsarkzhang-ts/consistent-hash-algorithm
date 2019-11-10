package moe.cnkirito.consistenthash;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author daofeng.xjf
 * @date 2019/2/15
 */
public class KetamaConsistentHashLoadBalancer implements LoadBalancer {

    private static MessageDigest md5Digest;

    static {
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
    }

    /**
     *  虚拟结点的个数，虚拟结点一般以4个为一组计算，所以数量一般同4的倍数
     */
    private final static int VIRTUAL_NODE_SIZE = 12;

    /**
     *  服务器与虚拟结点的连接符
     */
    private final static String VIRTUAL_NODE_SUFFIX = "-";

    /**
     * 根据请求匹配服务器
     * @param servers 服务器数组
     * @param invocation 封装了调用的请求
     * @return 返回匹配的值
     */
    @Override
    public Server select(List<Server> servers, Invocation invocation) {
        long invocationHashCode = getHashCode(invocation.getHashKey());
        TreeMap<Long, Server> ring = buildConsistentHashRing(servers);
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
    private Server locate(TreeMap<Long, Server> ring, Long invocationHashCode) {
        // 向右找到第一个 key
        Map.Entry<Long, Server> locateEntry = ring.ceilingEntry(invocationHashCode);
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
    private TreeMap<Long, Server> buildConsistentHashRing(List<Server> servers) {
        TreeMap<Long, Server> virtualNodeRing = new TreeMap<>();
        for (Server server : servers) {
            for (int i = 0; i < VIRTUAL_NODE_SIZE / 4; i++) {
                byte[] digest = computeMd5(server.getUrl() + VIRTUAL_NODE_SUFFIX + i);
                for (int h = 0; h < 4; h++) {
                    // 将digest数组按每四个元素为一组，通过位操作产生一个最大32位的长整数，
                    // 一个16位的MD5，可以生成4个哈希值，即4个虚拟结点，所以一般将虚拟结点按照
                    // 4个为一组进行计算。
                    Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                            | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                            | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                            | (digest[h * 4] & 0xFF);
                    virtualNodeRing.put(k, server);

                }
            }
        }
        return virtualNodeRing;
    }

    /**
     * 计算字符串的Ketama哈希值
     * @param origin key值
     * @return 哈希值
     */
    private long getHashCode(String origin) {
        byte[] bKey = computeMd5(origin);
        // 使用前4个字符进行位运算得到32位的整数。
        long rv = ((long) (bKey[3] & 0xFF) << 24)
                | ((long) (bKey[2] & 0xFF) << 16)
                | ((long) (bKey[1] & 0xFF) << 8)
                | (bKey[0] & 0xFF);
        return rv;
    }

    /**
     * 根据key生成16位的MD5摘要，因此digest数组共16位
     * @param k key
     * @return MD5摘
     */
    private static byte[] computeMd5(String k) {
        MessageDigest md5;
        try {
            md5 = (MessageDigest) md5Digest.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("clone of MD5 not supported", e);
        }
        md5.update(k.getBytes());
        return md5.digest();
    }

}
