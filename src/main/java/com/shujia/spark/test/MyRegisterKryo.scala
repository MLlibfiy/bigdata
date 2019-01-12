package com.shujia.spark.test

import com.twitter.chill.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyRegisterKryo extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    //注册Student类
    //注册之后student类序列化的时候就会使用kryo
    //classOf 获取类对象
    kryo.register(classOf[Student])
    //可以同时注册多个
//    kryo.register()
  }
}