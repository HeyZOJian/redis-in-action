import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 游戏中的商品买卖市场
 */
public class Chapter04 {
    public static final void main(String[] args) {
        new Chapter04().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testListItem(conn, false);
        testPurchaseItem(conn);
        testBenchmarkUpdateToken(conn);
    }

    public void testListItem(Jedis conn, boolean nested) {
        if (!nested){
            System.out.println("\n----- testListItem -----");
        }

        System.out.println("We need to set up just enough state so that a user can list an item");
        String seller = "userX";
        String item = "itemX";
        conn.sadd("inventory:" + seller, item);
        Set<String> i = conn.smembers("inventory:" + seller);

        System.out.println("The user's inventory has:");
        for (String member : i){
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        System.out.println();

        System.out.println("Listing the item...");
        boolean l = listItem(conn, item, seller, 10);
        System.out.println("Listing the item succeeded? " + l);
        assert l;
        Set<Tuple> r = conn.zrangeWithScores("market:", 0, -1);
        System.out.println("The market contains:");
        for (Tuple tuple : r){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert r.size() > 0;
    }

    public void testPurchaseItem(Jedis conn) {
        System.out.println("\n----- testPurchaseItem -----");
        testListItem(conn, true);

        System.out.println("We need to set up just enough state so a user can buy an item");
        conn.hset("users:userY", "funds", "125");
        Map<String,String> r = conn.hgetAll("users:userY");
        System.out.println("The user has some money:");
        for (Map.Entry<String,String> entry : r.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;
        assert r.get("funds") != null;
        System.out.println();

        System.out.println("Let's purchase an item");
        boolean p = purchaseItem(conn, "userY", "itemX", "userX", 10);
        System.out.println("Purchasing an item succeeded? " + p);
        assert p;
        r = conn.hgetAll("users:userY");
        System.out.println("Their money is now:");
        for (Map.Entry<String,String> entry : r.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;

        String buyer = "userY";
        Set<String> i = conn.smembers("inventory:" + buyer);
        System.out.println("Their inventory is now:");
        for (String member : i){
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        assert i.contains("itemX");
        assert conn.zscore("market:", "itemX.userX") == null;
    }

    public void testBenchmarkUpdateToken(Jedis conn) {
        System.out.println("\n----- testBenchmarkUpdate -----");
        benchmarkUpdateToken(conn, 5);
    }

    /**
     * 将用户包裹中的商品添加到买卖市场
     * @param conn redis连接
     * @param itemId 商品ID
     * @param sellerId 用户ID
     * @param price 商品价格
     * @return
     */
    public boolean listItem(
            Jedis conn, String itemId, String sellerId, double price) {

        // 构造用户包裹键值
        String inventory = "inventory:" + sellerId;
        // 构造买卖市场有序集合中的成员（即商品在买卖市场中的ID）
        String item = itemId + '.' + sellerId;
        long end = System.currentTimeMillis() + 5000;

        // 5秒内无限重试
        while (System.currentTimeMillis() < end) {
            // WATCH 监视 inventory这个键值的任何变化
            conn.watch(inventory);
            // 判断当前包裹中是否还含有该商品
            if (!conn.sismember(inventory, itemId)){
                // 对连接进行重置，取消 WATCH 命令对所有 键值 的监视
                conn.unwatch();
                return false;
            }
            // 执行MULTI指令 开始 事务
            Transaction trans = conn.multi();

            // 将商品ID和价格添加到买卖市场的有序集合中
            trans.zadd("market:", price, item);

            // 将商品从用户的包裹中移除
            trans.srem(inventory, itemId);

            // 执行EXEC指令 执行 事务
            List<Object> results = trans.exec();
            // 返回null说明WATCH的某个键值发生了改变，事务执行失败（即用户包裹发生变化）
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null){
                // 事务重试
                continue;
            }
            return true;
        }
        // 重试超时 ，放弃操作
        return false;
    }

    /**
     * 购买商品
     * @param conn
     * @param buyerId
     * @param itemId
     * @param sellerId
     * @param lprice
     * @return
     */
    public boolean purchaseItem(
            Jedis conn, String buyerId, String itemId, String sellerId, double lprice) {
        // 构造买方用户 key
        String buyer = "users:" + buyerId;
        // 构造卖方用户 key
        String seller = "users:" + sellerId;
        // 构造买卖市场（有序集合）中成员--商品
        String item = itemId + '.' + sellerId;
        // 构造买方用户包裹 key
        String inventory = "inventory:" + buyerId;
        long end = System.currentTimeMillis() + 10000;
        // 10秒内可重试
        while (System.currentTimeMillis() < end){
            // 监视 买卖市场、买方的任何变化
            conn.watch("market:", buyer);
            // 获取商品价格
            double price = conn.zscore("market:", item);
            // 获取买方所持有的金钱
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));

            if (price != lprice || price > funds){
                // 价格发生了变化 或者 买方不够钱购买
                // 清除监视
                conn.unwatch();

                return false;
            }

            // 开始事务型流水线
            Transaction trans = conn.multi();
            // 1.增加卖方所持有的金钱数
            trans.hincrBy(seller, "funds", (int)price);
            // 2.减少买方所持有的金钱数
            trans.hincrBy(buyer, "funds", (int)-price);
            // 3.将商品添加到买方的包裹中
            trans.sadd(inventory, itemId);
            // 4.将商品从买卖市场中移除
            trans.zrem("market:", item);
            // 执行事务
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null){
                continue;
            }
            return true;
        }

        return false;
    }

    public void benchmarkUpdateToken(Jedis conn, int duration) {
        try{
            @SuppressWarnings("rawtypes")
            Class[] args = new Class[]{
                Jedis.class, String.class, String.class, String.class};
            Method[] methods = new Method[]{
                this.getClass().getDeclaredMethod("updateToken", args),
                this.getClass().getDeclaredMethod("updateTokenPipeline", args),
            };
            for (Method method : methods){
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + (duration * 1000);
                while (System.currentTimeMillis() < end){
                    count++;
                    method.invoke(this, conn, "token", "user", "item");
                }
                long delta = System.currentTimeMillis() - start;
                System.out.println(
                        method.getName() + ' ' +
                        count + ' ' +
                        (delta / 1000) + ' ' +
                        (count / (delta / 1000)));
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 普通版更新token（一次只发一条命令，可能发2-5条）
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);
        if (item != null) {
            conn.zadd("viewed:" + token, timestamp, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     * 使用 非事务型流水线 更新token（将多条命令汇集之后一次性发给redis服务器，减少与redis服务器的通信往返次数）
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateTokenPipeline(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        // 开启流水线
        Pipeline pipe = conn.pipelined();
        // 开始非事务型流水线
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null){
            pipe.zadd("viewed:" + token, timestamp, item);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item);
        }
        // 执行非事务性流水线
        pipe.exec();
    }
}
