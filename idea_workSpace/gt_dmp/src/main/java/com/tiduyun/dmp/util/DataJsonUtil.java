package com.tiduyun.dmp.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class DataJsonUtil {
    public static String getJsonString(String data, String key) {
        JSONObject jsonObject = JSON.parseObject(data);
        System.out.println("解析字段之前打印"+"\n"+data);
        String value = jsonObject.getString(key);
        System.out.println("解析字段完成");
        return value;
    }
}
