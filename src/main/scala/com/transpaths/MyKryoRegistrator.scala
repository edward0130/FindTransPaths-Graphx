package com.transpaths

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[NodeInfo])
    kryo.register(classOf[TransPath])
  }
}