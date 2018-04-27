/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.types

import org.apache.calcite.runtime.GeoFunctions.Geom
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer

/**
  * Geometry type info for [[org.apache.calcite.runtime.GeoFunctions.Geom]].
  */
class GeomTypeInfo extends TypeInformation[Geom] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1

  override def getTypeClass: Class[Geom] = classOf[Geom]

  override def isKeyType: Boolean = false

  override def createSerializer(config: ExecutionConfig): KryoSerializer[Geom] =
    new KryoSerializer[Geom](classOf[Geom], config)

  override def canEqual(obj: scala.Any): Boolean = obj.isInstanceOf[GeomTypeInfo]

  override def toString: String = "Geom"

  override def equals(obj: scala.Any): Boolean = obj.isInstanceOf[GeomTypeInfo]

  override def hashCode(): Int = classOf[Geom].hashCode()

}

object GeomTypeInfo {
  final val GEOM = new GeomTypeInfo
}
