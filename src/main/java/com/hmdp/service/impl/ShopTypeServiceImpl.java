package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryType() {
        String key = CACHE_SHOPTYPE_KEY;
        //1.从redis查询商铺种类缓存
        String shoptypeJson = stringRedisTemplate.opsForValue().get(key);
        System.out.println(shoptypeJson);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shoptypeJson)) {
            //3.存在，直接返回，转为java对象
            //List<ShopType> list = JSONUtil.toBean(shoptypeJson, List.class);这里一开始不知道怎么转换
            List<ShopType> shopTypes = JSONUtil.toList(shoptypeJson, ShopType.class);
            return Result.ok(shopTypes);
        }
        //4.不存在，根据id查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        //5.数据库不存在，返回错误
        if(typeList==null){
            return Result.fail("店铺种类不存在");
        }
        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(typeList));
        //7.返回
        return Result.ok(typeList);
    }
}
