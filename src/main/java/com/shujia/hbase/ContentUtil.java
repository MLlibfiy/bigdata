package com.shujia.hbase;

import java.util.HashMap;

/**
 * 获取班级名和数字的对应关系
 */
public class ContentUtil {
    static HashMap<String, String> clazzMap = new HashMap<>();
    static HashMap<String, String> genderMap = new HashMap<>();

    static {
        clazzMap.put("文科一班", "1");
        clazzMap.put("文科二班", "2");
        clazzMap.put("文科三班", "3");
        clazzMap.put("文科四班", "4");
        clazzMap.put("文科五班", "5");
        clazzMap.put("文科六班", "6");

        clazzMap.put("理科一班", "a");
        clazzMap.put("理科二班", "b");
        clazzMap.put("理科三班", "c");
        clazzMap.put("理科四班", "d");
        clazzMap.put("理科五班", "e");
        clazzMap.put("理科六班", "f");


        genderMap.put("男", "1");
        genderMap.put("女", "0");

    }


    //获取性别对应的值
    public static String getGenderIndex(String gender){
        return genderMap.get(gender);
    }

    //获取班级对应的值
    public static String getClazzIndex(String clazz){
        return clazzMap.get(clazz);
    }



}
