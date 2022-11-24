package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;

import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    //提前定义好lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }



    //异步下单，创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //当前初始化完之后先立刻执行下面的方法
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable {
        String queueName="stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中得订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //解析消息中的订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //3.创建订单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    //从pending list中取出异常消息
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1.获取pending-list中得订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1如果获取失败，说明pending-list没有异常消息，结束循环
                        break;
                    }
                    //解析消息中的订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //3.创建订单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }
//    //创建阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    //线程任务
//    private class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //1.获取队列中得订单信息,不用担心while true，阻塞队列倘若没有元素就卡在这了
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁（对于异步操作来说，不太可能有并发问题，这里保险起见了）
        boolean isLock = lock.tryLock();
        //4.判断是否获取锁成功
        if (!isLock) {
            //获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try {
            //获得当前代理对象(事务)
            proxy.createVoucherOrderNew(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }

    }

    private IVoucherOrderService proxy;


    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //订单id，用id生成器生成
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //拿到代理对象，阻塞队列由另一个子线程进行，没法拿代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id，到这里为止，业务就结束了，用户得到了结果，异步下单的任务再单独进行即可
        return Result.ok(orderId);
        //到这里为止，仅仅是用户端判断完毕，已经往队列中发送了消息
        //接下来需要开启一个线程任务，去尝试获取消息队列中的信息
    }








//    @Override
    //注释掉的部分是用阻塞队列写的，改为用消息队列
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        //2.判断结果是否为0
//        int r = result.intValue();
//        if (r != 0) {
//            //2.1 不为0，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//        //2.2 为0，有购买资格，把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //2.3 订单id，用id生成器生成
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //2.4 用户id
//        voucherOrder.setUserId(userId);
//        //2.5 代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //2.6 放入阻塞队列
//        orderTasks.add(voucherOrder);
//        //拿到代理对象，阻塞队列由另一个子线程进行，没法拿代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //3.返回订单id，到这里为止，业务就结束了，用户得到了结果，异步下单的任务再单独进行即可
//        return Result.ok(orderId);
//    }
//    @Override
//    由于要根据lua脚本重新写，所以下面部分注释掉了
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            //尚未开始
//            return Result.fail("秒杀尚未开始！");
//        }
//        //3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            //已经结束
//            return Result.fail("秒杀已经结束！");
//        }
//        //4.判断库存是否充足
//        if (voucher.getStock()<1){
//            //库存不足
//            return Result.fail("库存不足！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        //使用Redisson的锁就不用自己再定义了，而且同样可以达到之前的效果
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否获取锁成功
//        if(!isLock){
//            //获取锁失败，返回错误或重试
//            return Result.fail("一个人只允许下一单");
//        }
//        try {
//            //获得当前代理对象(事务)
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            //释放锁
//            lock.unlock();
//        }
//    }
    //在这里加锁是为了把整个事务包括进去，要不然锁都解开了，事务还没提交，
    //此时再进来其他线程判断到的count还是0，只有事务提交了数据库才会改
//        synchronized(userId.toString().intern()) {
//            //获得当前代理对象(事务)
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }
    //但这里存在一个问题！！即事务失效
    //事务加给了createVoucherOrder函数，而不不包括上面部分，在上面部分调用时
    //实际是return this.createVoucherOrder(voucherId);
    //这里的this拿到的是VoucherOrderServiceImpl对象，而不是代理对象，事务管理的是代理对象
    //


    //事务能够起效，是因为springboot做了动态代理，拿到了下面函数的代理对象

    @Transactional
    public void createVoucherOrderNew(VoucherOrder voucherOrder) {
        //5.一人一单
        Long userId = voucherOrder.getUserId();
        //这里synchronized加锁应当只针对于相同的user对象
        //而相同user在进行创建时，及时toString其实也是一个新的字符串
        //因此要使用intern，判断值是否一样，一样则说明是同一个用户
        //synchronized(userId.toString().intern()) {  但是这里存在事务的问题，因此锁应该加在上面
        //5.1查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //5.2判断是否存在
        if (count > 0) {
            //用户已经购买过了
            log.error("用户已经购买过一次");
            return;
        }

        //6.扣减库存,包括设置sql语句和where条件
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1") //set stock = stock-1
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)//where id = ? and stock > 0
                .update();
        if (!success) {
            //扣减失败
            log.error("库存不足");
            return;
        }

        //倘若count不大于0，则可以继续创建订单
        //7.创建订单
        save(voucherOrder);
        //异步执行，不需要返回id
    }


    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //5.一人一单
        Long userId = UserHolder.getUser().getId();
        //这里synchronized加锁应当只针对于相同的user对象
        //而相同user在进行创建时，及时toString其实也是一个新的字符串
        //因此要使用intern，判断值是否一样，一样则说明是同一个用户
        //synchronized(userId.toString().intern()) {  但是这里存在事务的问题，因此锁应该加在上面
        //5.1查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        //5.2判断是否存在
        if (count > 0) {
            //用户已经购买过了
            return Result.fail("用户已经购买过一次！");
        }

        //6.扣减库存,包括设置sql语句和where条件
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1") //set stock = stock-1
                .eq("voucher_id", voucherId).gt("stock", 0)//where id = ? and stock > 0
                .update();
        if (!success) {
            //扣减失败
            return Result.fail("库存不足");
        }

        //倘若count不大于0，则可以继续创建订单
        //7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1订单id，用id生成器生成
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.2用户id

        voucherOrder.setUserId(userId);
        //7.3代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        //8.返回订单Id
        return Result.ok(orderId);

    }
}
