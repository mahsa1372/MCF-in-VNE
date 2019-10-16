package org.apache.spark.mllib.optimization.mip.lp

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class LPKryoRegistator extends KryoRegistrator {

	override def registerClasses(kryo: Kryo) {
		kryo.register(classOf[SimplexReduction])
	}
}
