import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Chapter06 {
    public static final void main(String[] args)
        throws Exception
    {
        new Chapter06().run();
    }

    public void run()
        throws InterruptedException, IOException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testAddUpdateContact(conn);
        testAddressBookAutocomplete(conn);
        testDistributedLocking(conn);
        testCountingSemaphore(conn);
        testDelayedTasks(conn);
        testMultiRecipientMessaging(conn);
        testFileDistribution(conn);
    }

    public void testAddUpdateContact(Jedis conn) {
        System.out.println("\n----- testAddUpdateContact -----");
        conn.del("recent:user");

        System.out.println("Let's add a few contacts...");
        for (int i = 0; i < 10; i++){
            addUpdateContact(conn, "user", "contact-" + ((int)Math.floor(i / 3)) + '-' + i);
        }
        System.out.println("Current recently contacted contacts");
        List<String> contacts = conn.lrange("recent:user", 0, -1);
        for(String contact : contacts){
            System.out.println("  " + contact);
        }
        assert contacts.size() >= 10;
        System.out.println();

        System.out.println("Let's pull one of the older ones up to the front");
        addUpdateContact(conn, "user", "contact-1-4");
        contacts = conn.lrange("recent:user", 0, 2);
        System.out.println("New top-3 contacts:");
        for(String contact : contacts){
            System.out.println("  " + contact);
        }
        assert "contact-1-4".equals(contacts.get(0));
        System.out.println();

        System.out.println("Let's remove a contact...");
        removeContact(conn, "user", "contact-2-6");
        contacts = conn.lrange("recent:user", 0, -1);
        System.out.println("New contacts:");
        for(String contact : contacts){
            System.out.println("  " + contact);
        }
        assert contacts.size() >= 9;
        System.out.println();

        System.out.println("And let's finally autocomplete on ");
        List<String> all = conn.lrange("recent:user", 0, -1);
        contacts = fetchAutocompleteList(conn, "user", "c");
        assert all.equals(contacts);
        List<String> equiv = new ArrayList<String>();
        for (String contact : all){
            if (contact.startsWith("contact-2-")){
                equiv.add(contact);
            }
        }
        contacts = fetchAutocompleteList(conn, "user", "contact-2-");
        Collections.sort(equiv);
        Collections.sort(contacts);
        assert equiv.equals(contacts);
        conn.del("recent:user");
    }

    public void testAddressBookAutocomplete(Jedis conn) {
        System.out.println("\n----- testAddressBookAutocomplete -----");
        conn.del("members:test");
        System.out.println("the start/end range of 'abc' is: " +
            Arrays.toString(findPrefixRange("abc")));
        System.out.println();

        System.out.println("Let's add a few people to the guild");
        for (String name : new String[]{"jeff", "jenny", "jack", "jennifer"}){
            joinGuild(conn, "test", name);
        }
        System.out.println();
        System.out.println("now let's try to find users with names starting with 'je':");
        Set<String> r = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(r);
        assert r.size() == 3;

        System.out.println("jeff just left to join a different guild...");
        leaveGuild(conn, "test", "jeff");
        r = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(r);
        assert r.size() == 2;
        conn.del("members:test");
    }

    public void testDistributedLocking(Jedis conn)
        throws InterruptedException
    {
        System.out.println("\n----- testDistributedLocking -----");
        conn.del("lock:testlock");
        System.out.println("Getting an initial lock...");
        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
        System.out.println("Got it!");
        System.out.println("Trying to get it again without releasing the first one...");
        assert acquireLockWithTimeout(conn, "testlock", 10, 1000) == null;
        System.out.println("Failed to get it!");
        System.out.println();

        System.out.println("Waiting for the lock to timeout...");
        Thread.sleep(2000);
        System.out.println("Getting the lock again...");
        String lockId = acquireLockWithTimeout(conn, "testlock", 1000, 1000);
        assert lockId != null;
        System.out.println("Got it!");
        System.out.println("Releasing the lock...");
        assert releaseLock(conn, "testlock", lockId);
        System.out.println("Released it...");
        System.out.println();

        System.out.println("Acquiring it again...");
        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
        System.out.println("Got it!");
        conn.del("lock:testlock");
    }

    public void testCountingSemaphore(Jedis conn)
        throws InterruptedException
    {
        System.out.println("\n----- testCountingSemaphore -----");
        conn.del("testsem", "testsem:owner", "testsem:counter");
        System.out.println("Getting 3 initial semaphores with a limit of 3...");
        for (int i = 0; i < 3; i++) {
            assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
        }
        System.out.println("Done!");
        System.out.println("Getting one more that should fail...");
        assert acquireFairSemaphore(conn, "testsem", 3, 1000) == null;
        System.out.println("Couldn't get it!");
        System.out.println();

        System.out.println("Lets's wait for some of them to time out");
        Thread.sleep(2000);
        System.out.println("Can we get one?");
        String id = acquireFairSemaphore(conn, "testsem", 3, 1000);
        assert id != null;
        System.out.println("Got one!");
        System.out.println("Let's release it...");
        assert releaseFairSemaphore(conn, "testsem", id);
        System.out.println("Released!");
        System.out.println();
        System.out.println("And let's make sure we can get 3 more!");
        for (int i = 0; i < 3; i++) {
            assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
        }
        System.out.println("We got them!");
        conn.del("testsem", "testsem:owner", "testsem:counter");
    }

    public void testDelayedTasks(Jedis conn)
        throws InterruptedException
    {
        System.out.println("\n----- testDelayedTasks -----");
        conn.del("queue:tqueue", "delayed:");
        System.out.println("Let's start some regular and delayed tasks...");
        for (long delay : new long[]{0, 500, 0, 1500}){
            assert executeLater(conn, "tqueue", "testfn", new ArrayList<String>(), delay) != null;
        }
        long r = conn.llen("queue:tqueue");
        System.out.println("How many non-delayed tasks are there (should be 2)? " + r);
        assert r == 2;
        System.out.println();

        System.out.println("Let's start up a thread to bring those delayed tasks back...");
        PollQueueThread thread = new PollQueueThread();
        thread.start();
        System.out.println("Started.");
        System.out.println("Let's wait for those tasks to be prepared...");
        Thread.sleep(2000);
        thread.quit();
        thread.join();
        r = conn.llen("queue:tqueue");
        System.out.println("Waiting is over, how many tasks do we have (should be 4)? " + r);
        assert r == 4;
        conn.del("queue:tqueue", "delayed:");
    }

    public void testMultiRecipientMessaging(Jedis conn) {
        System.out.println("\n----- testMultiRecipientMessaging -----");
        conn.del("ids:chat:", "msgs:1", "ids:1", "seen:joe", "seen:jeff", "seen:jenny");

        System.out.println("Let's create a new chat session with some recipients...");
        Set<String> recipients = new HashSet<String>();
        recipients.add("jeff");
        recipients.add("jenny");
        String chatId = createChat(conn, "joe", recipients, "message 1");
        System.out.println("Now let's send a few messages...");
        for (int i = 2; i < 5; i++){
            sendMessage(conn, chatId, "joe", "message " + i);
        }
        System.out.println();

        System.out.println("And let's get the messages that are waiting for jeff and jenny...");
        List<ChatMessages> r1 = fetchPendingMessages(conn, "jeff");
        List<ChatMessages> r2 = fetchPendingMessages(conn, "jenny");
        System.out.println("They are the same? " + r1.equals(r2));
        assert r1.equals(r2);
        System.out.println("Those messages are:");
        for(ChatMessages chat : r1){
            System.out.println("  chatId: " + chat.chatId);
            System.out.println("    messages:");
            for(Map<String,Object> message : chat.messages){
                System.out.println("      " + message);
            }
        }

        conn.del("ids:chat:", "msgs:1", "ids:1", "seen:joe", "seen:jeff", "seen:jenny");
    }

    public void testFileDistribution(Jedis conn)
        throws InterruptedException, IOException
    {
        System.out.println("\n----- testFileDistribution -----");
        String[] keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0){
            conn.del(keys);
        }
        conn.del(
            "msgs:test:",
            "seen:0",
            "seen:source",
            "ids:test:",
            "chat:test:");

        System.out.println("Creating some temporary 'log' files...");
        File f1 = File.createTempFile("temp_redis_1_", ".txt");
        f1.deleteOnExit();
        Writer writer = new FileWriter(f1);
        writer.write("one line\n");
        writer.close();

        File f2 = File.createTempFile("temp_redis_2_", ".txt");
        f2.deleteOnExit();
        writer = new FileWriter(f2);
        for (int i = 0; i < 100; i++){
            writer.write("many lines " + i + '\n');
        }
        writer.close();

        File f3 = File.createTempFile("temp_redis_3_", ".txt.gz");
        f3.deleteOnExit();
        writer = new OutputStreamWriter(
            new GZIPOutputStream(
                new FileOutputStream(f3)));
        Random random = new Random();
        for (int i = 0; i < 1000; i++){
            writer.write("random line " + Long.toHexString(random.nextLong()) + '\n');
        }
        writer.close();

        long size = f3.length();
        System.out.println("Done.");
        System.out.println();
        System.out.println("Starting up a thread to copy logs to redis...");
        File path = f1.getParentFile();
        CopyLogsThread thread = new CopyLogsThread(path, "test:", 1, size);
        thread.start();

        System.out.println("Let's pause to let some logs get copied to Redis...");
        Thread.sleep(250);
        System.out.println();
        System.out.println("Okay, the logs should be ready. Let's process them!");

        System.out.println("Files should have 1, 100, and 1000 lines");
        TestCallback callback = new TestCallback();
        processLogsFromRedis(conn, "0", callback);
        System.out.println(Arrays.toString(callback.counts.toArray(new Integer[0])));
        assert callback.counts.get(0) == 1;
        assert callback.counts.get(1) == 100;
        assert callback.counts.get(2) == 1000;

        System.out.println();
        System.out.println("Let's wait for the copy thread to finish cleaning up...");
        thread.join();
        System.out.println("Done cleaning out Redis!");

        keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0){
            conn.del(keys);
        }
        conn.del(
            "msgs:test:",
            "seen:0",
            "seen:source",
            "ids:test:",
            "chat:test:");
    }

    public class TestCallback
        implements Callback
    {
        private int index;
        public List<Integer> counts = new ArrayList<Integer>();

        public void callback(String line){
            if (line == null){
                index++;
                return;
            }
            while (counts.size() == index){
                counts.add(0);
            }
            counts.set(index, counts.get(index) + 1);
        }
    }

    /**
     * 添加最近联系人
     * @param conn
     * @param user
     * @param contact
     */
    public void addUpdateContact(Jedis conn, String user, String contact) {
        // 构造用户最近联系人 key
        String acList = "recent:" + user;
        Transaction trans = conn.multi();
        // 将该联系人从最近联系人中移除
        trans.lrem(acList, 0, contact);
        // 将该联系人压入最近联系人中的第一个
        trans.lpush(acList, contact);
        // 修剪最近联系人的大小 只保存前100个
        trans.ltrim(acList, 0, 99);
        trans.exec();
    }

    /**
     * 将指定联系人从联系人列表里移除
     * @param conn
     * @param user
     * @param contact
     */
    public void removeContact(Jedis conn, String user, String contact) {
        /**
         * 这个0是啥？
         * LREM KEY COUNT VALUE
         * 删除value 删剩下count个
         */
        conn.lrem("recent:" + user, 0, contact);
    }

    /**
     * 获取最近联系人自动补全列表
     * @param conn
     * @param user 用户ID
     * @param prefix 用户输入的联系人前缀
     * @return
     */
    public List<String> fetchAutocompleteList(Jedis conn, String user, String prefix) {
        // 获取该用户的最近联系人
        List<String> candidates = conn.lrange("recent:" + user, 0, -1);
        List<String> matches = new ArrayList<String>();
        // 匹配用户输入的前缀
        for (String candidate : candidates) {
            if (candidate.toLowerCase().startsWith(prefix)){
                matches.add(candidate);
            }
        }
        // 返回过滤后的结果
        return matches;
    }

    /**
     * 构造前缀的前驱和后继
     * 将前缀的最后一个元素 替换 成第一个排在该字符前面的字符，并加上一个{（为了在多个前缀搜索同时进行时，插入多个前驱引起前驱计算错误的问题）---前驱
     * 将前缀的最后一个元素 添加 第一个排在该字符后面的字符{（在只有a-z字符的情况下）-----后继
     */
    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";
    public String[] findPrefixRange(String prefix) {
        // 找到前缀最后一个字符在VALID_CHARACTERS的位置
        int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        String start = prefix.substring(0, prefix.length() - 1) + suffix + '{';
        String end = prefix + '{';
        return new String[]{start, end};
    }

    /**
     * 用户加入公会
     * @param conn
     * @param guild
     * @param user
     */
    public void joinGuild(Jedis conn, String guild, String user) {
        conn.zadd("members:" + guild, 0, user);
    }

    /**
     * 用户退出公会
     * @param conn
     * @param guild
     * @param user
     */
    public void leaveGuild(Jedis conn, String guild, String user) {
        conn.zrem("members:" + guild, user);
    }

    /**
     * 在redis中匹配前缀
     * @param conn
     * @param guild 公会ID
     * @param prefix
     * @return
     */
    @SuppressWarnings("unchecked")
    public Set<String> autocompleteOnPrefix(Jedis conn, String guild, String prefix) {
        // 计算前缀的前驱后继
        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];
        // 生成128位全局唯一的标识符（UUID）
        String identifier = UUID.randomUUID().toString();
        // 防止其他用户在查询匹配的时候错误的删除前驱后继，故添加UUID
        start += identifier;
        end += identifier;
        // 构造公会有序集合 key
        String zsetName = "members:" + guild;

        // 将前驱后继添加进公会有序集合
        conn.zadd(zsetName, 0, start);
        conn.zadd(zsetName, 0, end);

        Set<String> items = null;
        while (true){
            // 监视公会有序集合的变化
            conn.watch(zsetName);
            // 获取前驱的排名
            int sindex = conn.zrank(zsetName, start).intValue();
            // 获取后继的排名
            int eindex = conn.zrank(zsetName, end).intValue();
            // 默认只取10个元素（去掉前驱后继）时的结果个数，即去掉前驱后继后以prefix为前缀的最后一个元素的排名
            int erange = Math.min(sindex + 9, eindex - 2);
            // 开启事务
            Transaction trans = conn.multi();
            // 将前驱后继从有序列表中移除
            trans.zrem(zsetName, start);
            trans.zrem(zsetName, end);
            // 移除前驱后sindex就是以prefix为前缀的第一个记录的排名了
            trans.zrange(zsetName, sindex, erange);
            // 执行事务
            List<Object> results = trans.exec();
            if (results != null){
                items = (Set<String>)results.get(results.size() - 1);
                break;
            }
        }
        // 移除结果中带花括号的元素，移除其他自动补全操作的影响（即其他操作在本操作watch前在前驱后继中间插入了其他操作的前驱后继）
        for (Iterator<String> iterator = items.iterator(); iterator.hasNext(); ){
            if (iterator.next().indexOf('{') != -1){
                iterator.remove();
            }
        }
        return items;
    }

    /**
     * v1.0 SETNX实现的加锁操作
     * SETNX只会在键值不存在的情况下为键赋值
     * @param conn
     * @param lockName
     * @return
     */
    public String acquireLock(Jedis conn, String lockName) {
        return acquireLock(conn, lockName, 10000);
    }
    public String acquireLock(Jedis conn, String lockName, long acquireTimeout){
        // 生成全局唯一标识符UUID 来为键值赋值表示获取锁
        String identifier = UUID.randomUUID().toString();

        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end){
            // 尝试获取锁
            if (conn.setnx("lock:" + lockName, identifier) == 1){
                return identifier;
            }

            try {
                Thread.sleep(1);
            }catch(InterruptedException ie){
                Thread.currentThread().interrupt();
            }
        }

        return null;
    }

    /**
     * v2.0 SETNX实现的带有超时限制的加锁操作
     * 确保锁在持有者崩溃的情况下也能自动被释放
     * @param conn
     * @param lockName
     * @param acquireTimeout 加锁超时重试时限
     * @param lockTimeout 锁的生存时间
     * @return
     */
    public String acquireLockWithTimeout(
        Jedis conn, String lockName, long acquireTimeout, long lockTimeout)
    {
        String identifier = UUID.randomUUID().toString();
        String lockKey = "lock:" + lockName;
        int lockExpire = (int)(lockTimeout / 1000);

        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockKey, identifier) == 1){
                // 获取锁成功， 并为锁设置过期时间
                conn.expire(lockKey, lockExpire);
                return identifier;
            }
            if (conn.ttl(lockKey) == -1) {
               // 检查过期时间，若没有设置过期时间，则马上设置锁的过期时间
                conn.expire(lockKey, lockExpire);
            }

            try {
                Thread.sleep(1);
            }catch(InterruptedException ie){
                Thread.currentThread().interrupt();
            }
        }

        // null indicates that the lock was not acquired
        return null;
    }

    /**
     * V1.0 解锁操作
     * @param conn
     * @param lockName
     * @param identifier
     * @return
     */
    public boolean releaseLock(Jedis conn, String lockName, String identifier) {
        // 构造锁 key
        String lockKey = "lock:" + lockName;

        while (true){
            // 监视锁的变化
            conn.watch(lockKey);
            // 检查键目前的值是否和加锁时设的值相同，锁是否被持有
            if (identifier.equals(conn.get(lockKey))){
                // 开始事务
                Transaction trans = conn.multi();
                // 删除该键，即解锁
                trans.del(lockKey);
                // 执行事务
                List<Object> results = trans.exec();
                if (results == null){
                    continue;
                }
                return true;
            }

            conn.unwatch();
            break;
        }

        return false;
    }

    /**
     * 实现公平信号量
     * @param conn
     * @param semname 超时有序集合
     * @param limit
     * @param timeout
     * @return
     */
    public String acquireFairSemaphore(
        Jedis conn, String semname, int limit, long timeout)
    {
        /**
         * 超时有序集合 作用：移除超时的信号量
         * 信号量拥有者有序集合 作用：判断是否成功获取信号量
         */
        String identifier = UUID.randomUUID().toString();
        // 信号量拥有者 有序集合 key
        String czset = semname + ":owner";
        // 计数器 key
        String ctr = semname + ":counter";

        long now = System.currentTimeMillis();
        // 开启事务
        Transaction trans = conn.multi();
        // ZREMRANGEBYSCORE key min max
        // 删除过期的标识符
        trans.zremrangeByScore(
            semname.getBytes(),// key
            "-inf".getBytes(),// min
            String.valueOf(now - timeout).getBytes());// max
        ZParams params = new ZParams();
        params.weights(1, 0);
        // 超时有序集合与信号量拥有者有序集合执行交集运算
        trans.zinterstore(czset, params, czset, semname);
        // 更新计数器
        trans.incr(ctr);
        List<Object> results = trans.exec();
        // 获取计数器当前的最大值
        int counter = ((Long)results.get(results.size() - 1)).intValue();

        // 开启事务
        trans = conn.multi();
        // 将标识符和时间戳添加进超时有序集合中
        trans.zadd(semname, now, identifier);
        // 将标识符和从计数器获取的值添加进信号量拥有者有序集合中
        trans.zadd(czset, counter, identifier);
        trans.zrank(czset, identifier);
        results = trans.exec();
        // 查询当前标识符在信号量拥有者有序集合中的排名
        int result = ((Long)results.get(results.size() - 1)).intValue();
        if (result < limit){
            // 获取信号量成功， 放回标识符UUID
            return identifier;
        }
        // 计数信号量和其他锁的区别在于，当客户端获取锁失败时，客户端通常会选择等待，
        // 而当客户端获取计数信号量失败的时候，客户端通常会选择立即返回失败结果
        // 获取信号量失败，将标识符从 超时有序集合和信号量拥有者有序集合中 删除
        trans = conn.multi();
        trans.zrem(semname, identifier);
        trans.zrem(czset, identifier);
        trans.exec();
        return null;
    }

    /**
     * 公平信号量释放操作
     * @param conn
     * @param semname
     * @param identifier
     * @return
     */
    public boolean releaseFairSemaphore(
        Jedis conn, String semname, String identifier)
    {
        // 将标识符从 超时有序集合和信号量拥有者有序集合中 删除
        Transaction trans = conn.multi();
        trans.zrem(semname, identifier);
        trans.zrem(semname + ":owner", identifier);
        List<Object> results = trans.exec();
        return (Long)results.get(results.size() - 1) == 1;
    }

    /**
     * 添加延迟任务
     * @param conn
     * @param queue 任务队列名字
     * @param name 处理任务的回调函数的名字
     * @param args 回调函数参数列表
     * @param delay 延迟执行的时间
     * @return
     */
    public String executeLater(
        Jedis conn, String queue, String name, List<String> args, long delay)
    {
        Gson gson = new Gson();
        String identifier = UUID.randomUUID().toString();
        String itemArgs = gson.toJson(args);
        // 构造json数据
        String item = gson.toJson(new String[]{identifier, queue, name, itemArgs});
        if (delay > 0){
            // 添加到延迟队列有序集合中，并设置分值为延迟的时间
            conn.zadd("delayed:", System.currentTimeMillis() + delay, item);
        } else {
            // 立即添加到任务队列中
            conn.rpush("queue:" + queue, item);
        }
        return identifier;
    }

    public String createChat(Jedis conn, String sender, Set<String> recipients, String message) {
        // 通过全局计数器自增获取新群组的ID
        String chatId = String.valueOf(conn.incr("ids:chat:"));
        return createChat(conn, sender, recipients, message, chatId);
    }

    /**
     * 创建群组
     * @param conn
     * @param sender
     * @param recipients
     * @param message
     * @param chatId
     * @return
     */
    public String createChat(
        Jedis conn, String sender, Set<String> recipients, String message, String chatId)
    {
        recipients.add(sender);

        Transaction trans = conn.multi();
        for (String recipient : recipients){
            // 将参与群聊的用户添加到群组有序集合中
            trans.zadd("chat:" + chatId, 0, recipient);
            // 将群组ID添加进用户的已读有序集合中
            trans.zadd("seen:" + recipient, 0, chatId);
        }
        trans.exec();
        // 发送一条初始化消息
        return sendMessage(conn, chatId, sender, message);
    }

    /**
     * 向群组发送消息
     * @param conn
     * @param chatId
     * @param sender
     * @param message
     * @return
     */
    public String sendMessage(Jedis conn, String chatId, String sender, String message) {
        // 对群组加锁
        String identifier = acquireLock(conn, "chat:" + chatId);
        if (identifier == null){
            throw new RuntimeException("Couldn't get the lock");
        }
        try {
            // 更新消息计数器的值（这是一个竞争条件）
            long messageId = conn.incr("ids:" + chatId);
            // 构造消息json
            HashMap<String,Object> values = new HashMap<String,Object>();
            values.put("id", messageId);
            values.put("ts", System.currentTimeMillis());
            values.put("sender", sender);
            values.put("message", message);
            String packed = new Gson().toJson(values);
            // 将消息添加进消息有序集合中，成员为json数据，分值为消息ID
            conn.zadd("msgs:" + chatId, messageId, packed);
        }finally{
            releaseLock(conn, "chat:" + chatId, identifier);
        }
        return chatId;
    }

    @SuppressWarnings("unchecked")
    public List<ChatMessages> fetchPendingMessages(Jedis conn, String recipient) {
        // 获取用户所加入的群组ID和已读消息ID
        Set<Tuple> seenSet = conn.zrangeWithScores("seen:" + recipient, 0, -1);
        List<Tuple> seenList = new ArrayList<Tuple>(seenSet);

        Transaction trans = conn.multi();
        for (Tuple tuple : seenList){
            // 群组ID
            String chatId = tuple.getElement();
            // 已读消息ID
            int seenId = (int)tuple.getScore();
            trans.zrangeByScore("msgs:" + chatId, String.valueOf(seenId + 1), "inf");
        }
        // 获取所有群组的未读消息
        List<Object> results = trans.exec();

        Gson gson = new Gson();
        // 群组ID迭代器
        Iterator<Tuple> seenIterator = seenList.iterator();
        // 未读消息ID迭代器
        Iterator<Object> resultsIterator = results.iterator();

        List<ChatMessages> chatMessages = new ArrayList<ChatMessages>();
        // 保存更新后的用户群组有序集合已读消息的值
        List<Object[]> seenUpdates = new ArrayList<Object[]>();
        // 保存已经被所有人读取 需要删除的消息
        List<Object[]> msgRemoves = new ArrayList<Object[]>();
        // 获取每个群组中的未读消息
        while (seenIterator.hasNext()){
            Tuple seen = seenIterator.next();
            Set<String> messageStrings = (Set<String>)resultsIterator.next();
            if (messageStrings.size() == 0){
                continue;
            }

            int seenId = 0;
            String chatId = seen.getElement();
            // 解析json消息
            List<Map<String,Object>> messages = new ArrayList<Map<String,Object>>();
            for (String messageJson : messageStrings){
                Map<String,Object> message = (Map<String,Object>)gson.fromJson(
                    messageJson, new TypeToken<Map<String,Object>>(){}.getType());
                int messageId = ((Double)message.get("id")).intValue();
                if (messageId > seenId){
                    seenId = messageId;
                }
                message.put("id", messageId);
                messages.add(message);
            }
            // 更新群组有序集合中用户已读消息的值
            conn.zadd("chat:" + chatId, seenId, recipient);
            seenUpdates.add(new Object[]{"seen:" + recipient, seenId, chatId});

            Set<Tuple> minIdSet = conn.zrangeWithScores("chat:" + chatId, 0, 0);
            if (minIdSet.size() > 0){
                msgRemoves.add(new Object[]{
                    "msgs:" + chatId, minIdSet.iterator().next().getScore()});
            }
            chatMessages.add(new ChatMessages(chatId, messages));
        }

        trans = conn.multi();
        for (Object[] seenUpdate : seenUpdates){
            trans.zadd(
                (String)seenUpdate[0],
                (Integer)seenUpdate[1],
                (String)seenUpdate[2]);
        }
        for (Object[] msgRemove : msgRemoves){
            trans.zremrangeByScore(
                (String)msgRemove[0], 0, ((Double)msgRemove[1]).intValue());
        }
        trans.exec();

        return chatMessages;
    }

    public void processLogsFromRedis(Jedis conn, String id, Callback callback)
        throws InterruptedException, IOException
    {
        while (true){
            List<ChatMessages> fdata = fetchPendingMessages(conn, id);

            for (ChatMessages messages : fdata){
                for (Map<String,Object> message : messages.messages){
                    String logFile = (String)message.get("message");

                    if (":done".equals(logFile)){
                        return;
                    }
                    if (logFile == null || logFile.length() == 0){
                        continue;
                    }

                    InputStream in = new RedisInputStream(
                        conn, messages.chatId + logFile);
                    if (logFile.endsWith(".gz")){
                        in = new GZIPInputStream(in);
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try{
                        String line = null;
                        while ((line = reader.readLine()) != null){
                            callback.callback(line);
                        }
                        callback.callback(null);
                    }finally{
                        reader.close();
                    }

                    conn.incr(messages.chatId + logFile + ":done");
                }
            }

            if (fdata.size() == 0){
                Thread.sleep(100);
            }
        }
    }

    public class RedisInputStream
        extends InputStream
    {
        private Jedis conn;
        private String key;
        private int pos;

        public RedisInputStream(Jedis conn, String key){
            this.conn = conn;
            this.key = key;
        }

        @Override
        public int available()
            throws IOException
        {
            long len = conn.strlen(key);
            return (int)(len - pos);
        }

        @Override
        public int read()
            throws IOException
        {
            byte[] block = conn.substr(key.getBytes(), pos, pos);
            if (block == null || block.length == 0){
                return -1;
            }
            pos++;
            return (int)(block[0] & 0xff);
        }

        @Override
        public int read(byte[] buf, int off, int len)
            throws IOException
        {
            byte[] block = conn.substr(key.getBytes(), pos, pos + (len - off - 1));
            if (block == null || block.length == 0){
                return -1;
            }
            System.arraycopy(block, 0, buf, off, block.length);
            pos += block.length;
            return block.length;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    public interface Callback {
        void callback(String line);
    }

    public class ChatMessages
    {
        public String chatId;
        public List<Map<String,Object>> messages;

        public ChatMessages(String chatId, List<Map<String,Object>> messages){
            this.chatId = chatId;
            this.messages = messages;
        }

        public boolean equals(Object other){
            if (!(other instanceof ChatMessages)){
                return false;
            }
            ChatMessages otherCm = (ChatMessages)other;
            return chatId.equals(otherCm.chatId) &&
                messages.equals(otherCm.messages);
        }
    }

    public class PollQueueThread
        extends Thread
    {
        private Jedis conn;
        private boolean quit;
        private Gson gson = new Gson();

        public PollQueueThread(){
            this.conn = new Jedis("localhost");
            this.conn.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit){
                // 获取延迟队列有序集合中的第一个元素
                Set<Tuple> items = conn.zrangeWithScores("delayed:", 0, 0);
                Tuple item = items.size() > 0 ? items.iterator().next() : null;
                if (item == null || item.getScore() > System.currentTimeMillis()) {
                    // 队列中没有任务或者任务的执行时间尚未来临，则休眠一会
                    try{
                        sleep(10);
                    }catch(InterruptedException ie){
                        Thread.interrupted();
                    }
                    continue;
                }
                // 解析json
                String json = item.getElement();
                String[] values = gson.fromJson(json, String[].class);
                String identifier = values[0];
                String queue = values[1];
                // 对该延迟任务加锁
                String locked = acquireLock(conn, identifier);
                if (locked == null){
                    continue;
                }
                // 1.将任务从延迟队列中删除
                // 2.将任务添加进任务队列中
                if (conn.zrem("delayed:", json) == 1){
                    conn.rpush("queue:" + queue, json);
                }
                // 解锁
                releaseLock(conn, identifier, locked);
            }
        }
    }

    public class CopyLogsThread
        extends Thread
    {
        private Jedis conn;
        private File path;
        private String channel;
        private int count;
        private long limit;

        public CopyLogsThread(File path, String channel, int count, long limit) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.path = path;
            this.channel = channel;
            this.count = count;
            this.limit = limit;
        }

        public void run() {
            Deque<File> waiting = new ArrayDeque<File>();
            long bytesInRedis = 0;

            Set<String> recipients= new HashSet<String>();
            for (int i = 0; i < count; i++){
                recipients.add(String.valueOf(i));
            }
            createChat(conn, "source", recipients, "", channel);
            File[] logFiles = path.listFiles(new FilenameFilter(){
                public boolean accept(File dir, String name){
                    return name.startsWith("temp_redis");
                }
            });
            Arrays.sort(logFiles);
            for (File logFile : logFiles){
                long fsize = logFile.length();
                while ((bytesInRedis + fsize) > limit){
                    long cleaned = clean(waiting, count);
                    if (cleaned != 0){
                        bytesInRedis -= cleaned;
                    }else{
                        try{
                            sleep(250);
                        }catch(InterruptedException ie){
                            Thread.interrupted();
                        }
                    }
                }

                BufferedInputStream in = null;
                try{
                    in = new BufferedInputStream(new FileInputStream(logFile));
                    int read = 0;
                    byte[] buffer = new byte[8192];
                    while ((read = in.read(buffer, 0, buffer.length)) != -1){
                        if (buffer.length != read){
                            byte[] bytes = new byte[read];
                            System.arraycopy(buffer, 0, bytes, 0, read);
                            conn.append((channel + logFile).getBytes(), bytes);
                        }else{
                            conn.append((channel + logFile).getBytes(), buffer);
                        }
                    }
                }catch(IOException ioe){
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }finally{
                    try{
                        in.close();
                    }catch(Exception ignore){
                    }
                }

                sendMessage(conn, channel, "source", logFile.toString());

                bytesInRedis += fsize;
                waiting.addLast(logFile);
            }

            sendMessage(conn, channel, "source", ":done");

            while (waiting.size() > 0){
                long cleaned = clean(waiting, count);
                if (cleaned != 0){
                    bytesInRedis -= cleaned;
                }else{
                    try{
                        sleep(250);
                    }catch(InterruptedException ie){
                        Thread.interrupted();
                    }
                }
            }
        }

        private long clean(Deque<File> waiting, int count) {
            if (waiting.size() == 0){
                return 0;
            }
            File w0 = waiting.getFirst();
            if (String.valueOf(count).equals(conn.get(channel + w0 + ":done"))){
                conn.del(channel + w0, channel + w0 + ":done");
                return waiting.removeFirst().length();
            }
            return 0;
        }
    }
}
