package com.shujia.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * 读取数据工具
 */
public class DataUtil {

    public static <T> ArrayList<T> load(String path,Class<T> c) {
        ArrayList<T> lists = new ArrayList<T>();
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {//监控异常代码块
            fileReader = new FileReader(path);
            bufferedReader = new BufferedReader(fileReader);
            String line ;
            while ((line = bufferedReader.readLine())!=null){
                String[] split = line.split(",");
                //更具类对象创建对象
                T t = c.newInstance();
                //获取属性列表
                Field[] fields = c.getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    String value = split[i];
                    //获取属性名
                    String name = fields[i].getName();
                    //获取属性类型
                    Class type = fields[i].getType();
                    //拼接设置属性值的方法  setName()
                    String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
                    //根据方法名和参数类型获取方法的对象
                    Method method = c.getMethod(methodName, type);
                    //执行方法，给属性设置值

                    if (type.getName().equals("java.lang.Integer")){
                        method.invoke(t,Integer.parseInt(value));
                    }else {
                        method.invoke(t,value);
                    }
                }

                lists.add(t);
            }

        } catch (FileNotFoundException e) {//处理异常的代码块，如果出现了异常会执行eatch代码
            //打印堆栈信息，也就是异常信息
            System.out.println("文件找不到");
//            e.printStackTrace();
        } catch (Exception e) {//eatch根据异常类型捕获异常
            e.printStackTrace();
        } finally {//不管是否发生异常都会执行
            if (fileReader!=null){
                try {
                    fileReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (bufferedReader!=null){
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return lists;
    }
}
