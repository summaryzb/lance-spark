/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * The new SQL syntax is:
 *   -ALTER TABLE t ADD STATICS — compute statics for all columns
 *   -ALTER TABLE t ADD STATICS (col1, col2) — compute statics for specific columns
 *
 * @param table   The target table to add statics to
 * @param columns Optional list of column names to compute statics for; empty means all columns
 */
case class AddStatics(
    table: LogicalPlan,
    columns: Seq[String] = Seq.empty) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = Seq.empty

  override def simpleString(maxFields: Int): String = {
    s"AddStatics(columns=${columns.mkString(",")})"
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): AddStatics = {
    copy(table = newChildren(0), this.columns)
  }
}
