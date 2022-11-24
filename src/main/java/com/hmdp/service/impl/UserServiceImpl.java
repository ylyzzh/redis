package com.hmdp.service.impl;
import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;

import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    //短信登陆的第一模块-发送短信验证码
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号 在utils目录下的RegexPatterns里定义了手机、邮箱、密码、验证码等信息的正则表达式
        //还有一个RegexUtils，进行具体的校验
        if (RegexUtils.isPhoneInvalid(phone)){
            //2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
        //3.符合，生成验证码，需要hutool工具类
        String code = RandomUtil.randomNumbers(6);
        //4.保存到redis
        //session.setAttribute("code",code);
        //这里设置一个有效期两分钟，毕竟如果有人一直存，redis就爆满了，因此需要设置有效期
        //stringRedisTemplate.opsForValue().set("login:code:"+phone,code,2, TimeUnit.MINUTES);这样定义有点low，一般都是常量
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.发送验证码,先不做了，需要云平台一般，这里先记录一下
        log.debug("发送短信验证码成功，验证码：{}",code);
        return Result.ok();
    }

    @Override
    //短信登陆的第二模块-短信验证码登录、注册
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1. 校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)){
            //2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
        //3.从redis获取验证码并校验
//        Object cacheCode = session.getAttribute("code");
        String cacheCode=stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+phone);
        String code = loginForm.getCode();//这里是前端提交的code
        if(cacheCode==null||!cacheCode.equals(code)){
            //不一致，直接报错
            return Result.fail("验证码错误");
        }

        //4.一致，根据手机号查询用户 select * from tb_user where phone = ?
        User user = query().eq("phone", phone).one();
        //5.判断用户是否存在
        if(user==null){
            //6.不存在，创建新用户并保存
            user = createUserWithPhone(phone);
        }
        //7.保存用户信息到redis中
        //7.1随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);//true不带中划线
        //7.2将User对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
//        HashMap<String,String> userMap = new HashMap<>();
//        userMap.put("id",userDTO.getId()+"");
//        userMap.put("nickname",userDTO.getNickName());
//        userMap.put("icon",userDTO.getIcon());
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        //7.3存储
        String tokenKey = LOGIN_USER_KEY + token;
        //session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);

        //7.4 opsForHash没法直接设置有效期，需要单独设置token有效期
        //注意这里的token有限期不是固定30min，在拦截器里进行了刷新设置，用户只要有操作，就继续30min
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL, TimeUnit.MINUTES);
        //8.返回token给客户端
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key=USER_SIGN_KEY+userId+keySuffix;
        //4.获取今天是本月的第几天,获得的是从1-31，需要-1
        int dayOfMonth = now.getDayOfMonth();
        //5.写入Redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth-1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key=USER_SIGN_KEY+userId+keySuffix;
        //4.获取今天是本月的第几天,获得的是从1-31，需要-1
        int dayOfMonth = now.getDayOfMonth();
        //5.获取本月截至今天为止的所有的签到记录，返回的是一个十进制的数字 BITFIELD sign：5：202203 GET u14 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (result == null || result.isEmpty()) {
            //没有任何签到结果
            return Result.ok(0);
        }
        //这里只有一个操作get，index为0
        Long num = result.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        //6.循环遍历
        int count=0;
        while (true) {
            //6.1让这个数字与1做  与运算  ，得到数字的最后一个bit位
            //6.2判断这个bit位是否为0
            if ((num&1)==0) {
                //6.3如果为0，说明未签到，结束
                break;
            }else {
                //6.4如果不为0，说明已签到，计数器+1
                count++;
            }
            //6.5把数字右移一位，抛弃最后一个bit位，继续下一个bit位
            num=num>>1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        //1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        //2.保存用户
        save(user);
        return user;
    }
}
