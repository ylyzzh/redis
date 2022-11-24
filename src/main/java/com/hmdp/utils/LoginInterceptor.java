package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//拦截器写好了还不会生效，需要写一个config配置文件
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        //1.获取session
//        HttpSession session = request.getSession();
//        //2.获取session中的用户
//        Object user = session.getAttribute("user");
//        //3.判断用户是否存在
//        if(user==null){
//            //4.不存在，拦截
//            response.setStatus(401);
//            return  false;
//        }
//        //5.存在，保存用户信息到ThreadLocal
//        //注意ThreadLocal在utils下的UserHolder中创建好了
//        UserHolder.saveUser((UserDTO) user);
//        //6.放行
//        return true;

//        //1.获取请求头中的token
//        String token = request.getHeader("authorization");
//        if (StrUtil.isBlank(token)) {
//            //为空，拦截
//            response.setStatus(401);
//            return  false;
//        }
//        //2.基于token获取redis中的用户
//        String key = RedisConstants.LOGIN_USER_KEY + token;
//        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
//        //3.判断用户是否存在
//        if(userMap.isEmpty()){
//            //4.不存在，拦截
//            response.setStatus(401);
//            return  false;
//        }
//        //5.将查询到的Hash数据转化为UserDTO对象
//        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
//        //6.存在，保存用户信息到ThreadLocal
//        //注意ThreadLocal在utils下的UserHolder中创建好了
//        UserHolder.saveUser(userDTO);
//        //7.刷新token有效期,也就是用户只要有操作，token就要刷新30min
//        stringRedisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
//        //8.放行

        //1.判断是否需要拦截(ThreadLocal中是否有用户)
        if(UserHolder.getUser()==null){
            //没有，需要拦截
            response.setStatus(401);
            //拦截
            return false;
        }
        //有用户，则放行
        return true;

    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        //移除用户
        UserHolder.removeUser();
    }
}
