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

package org.apache.flink.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.runtime.types.GeomTypeInfo

/**
  * Geom Expressions
  */

case class STPointFromText(wkt: Expression) extends Expression with InputTypeSpec {
  override private[flink] def expectedTypes = STRING_TYPE_INFO :: Nil

  override private[flink] def resultType = Types.GEOM

  override def toString: String = s"stPointFromText($wkt)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.ST_POINT_FROM_TEXT, wkt.toRexNode)
  }

  override private[flink] def children = Seq(wkt)
}

case class STAsText(geom: Expression) extends Expression with InputTypeSpec {
  override private[flink] def expectedTypes = Types.GEOM :: Nil

  override private[flink] def resultType = STRING_TYPE_INFO

  override def toString: String = s"stAsText($geom)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.ST_AS_TEXT, geom.toRexNode)
  }

  override private[flink] def children = Seq(geom)
}

case class STGeomFromText(wkt: Expression, id: Expression*)
  extends Expression with InputTypeSpec {
  override private[flink] def expectedTypes =
    if (id.nonEmpty) {
      STRING_TYPE_INFO :: INT_TYPE_INFO :: Nil
    } else {
      STRING_TYPE_INFO :: Nil
    }

  override private[flink] def resultType = Types.GEOM

  override def toString: String =
    if (id.nonEmpty) {
      s"geomFromText($wkt, ${id(0)}"
    } else {
      s"geomFromText($wkt)"
    }

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    if (id.nonEmpty) {
      relBuilder.call(ScalarSqlFunctions.ST_GEOM_FROM_TEXT, wkt.toRexNode, id(0).toRexNode)
    } else {
      relBuilder.call(ScalarSqlFunctions.ST_GEOM_FROM_TEXT, wkt.toRexNode)
    }

  override private[flink] def children =
    if (id.nonEmpty) {
      Seq(wkt, id(0))
    } else {
      Seq(wkt)
    }
}
