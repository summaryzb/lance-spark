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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{BooleanType, CharType, DataType, DatetimeType, DecimalType, DoubleType, FloatType, IntegralType, StringType, VarcharType}
import org.lance.spark.LanceDataset
import org.lance.spark.read.LanceStatistics

/**
 * similar to org.apache.spark.sql.execution.command.AnalyzeColumnCommand
 */
case class AddStaticsExec(
    catalog: TableCatalog,
    ident: Identifier,
    columns: Seq[String]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case d: LanceDataset => d
      case _ => throw new UnsupportedOperationException("AddIndex only supports LanceDataset")
    }

    val relation = DataSourceV2Relation.create(lanceDataset, Some(catalog), Some(ident))

    val columnsToAnalyze = getColumnsToAnalyze(relation, Option(columns), columns.isEmpty)

    val (_, newColStats) =
      org.apache.spark.sql.execution.command.CommandUtils
        .computeColumnStats(session, relation, columnsToAnalyze)

    val properties: Map[String, String] = newColStats.flatMap {
      case (attr, columnStat) =>
        columnStat.toCatalogColumnStat(attr.name, attr.dataType).toMap(attr.name)
    }
    val changes = properties.map {
      case (k, v) => TableChange.setProperty(LanceStatistics.STATS_PREFIX + k, v)
    }.toArray
    catalog.alterTable(ident, changes: _*)

    Seq.empty
  }

  private def getColumnsToAnalyze(
      relation: LogicalPlan,
      columnNames: Option[Seq[String]],
      allColumns: Boolean = false): Seq[Attribute] = {
    val columnsToAnalyze = if (allColumns) {
      relation.output
    } else {
      columnNames.get.map { col =>
        val exprOption = relation.output.find(attr => conf.resolver(attr.name, col))
        exprOption.getOrElse(throw QueryCompilationErrors.columnNotFoundError(col))
      }
    }
    val metaColName = LanceDataset.METADATA_COLUMNS.map(_.name)
    // Make sure the column types are supported for stats gathering.
    columnsToAnalyze
      .filter(attr => !metaColName.contains(attr.name))
      .filter(attr => supportsType(attr.dataType))
  }

  private def supportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case _: DatetimeType => true
    case CharType(_) | VarcharType(_) => false
    case StringType => true
    case _ => false
  }
}
