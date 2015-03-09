/*
* Copyright 2015 eleflow.com.br.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package eleflow.sparknotebook.data

import java.sql.Timestamp

import com.gensler.scalavro.util.Union
import eleflow.sparknotebook.enums.DataSetType
import eleflow.sparknotebook.exception.UnexpectedValueException
import eleflow.sparknotebook.data.Dataset._
import eleflow.sparknotebook.util.IntStringImplicitTypeConverter.IS
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, _}


/**
 * Created by dirceu on 16/10/14.
 */

object DataTransformer {

  def createLabeledPointFromRDD(schemaRDD: Dataset, target: Seq[Int], datasetType: DataSetType.Types): RDD[(Map[Double,Any], LabeledPoint)] = {
    createLabeledPointFromRDD(schemaRDD, target, schemaRDD.summarizedColumns, datasetType, schemaRDD.columnsSize.toInt - 1)
  }

  def createLabeledPointFromRDD(rdd: Dataset, target: Seq[Int],
                                normalized: RDD[(Int, (Int, (Any => Int), (Any => Double)))],
                                dataSetType: DataSetType.Types, columnsSize: Int): RDD[(Map[Double,Any], LabeledPoint)] = {
    val (fields, idField) = rdd.schema.fields.zipWithIndex.partition(f => !target.contains(f._2))
    val normalizedStrings = rdd.context.broadcast(normalized.collectAsMap())
    rdd.zipWithIndex.map {
      case (row, rowIndex) =>
        val norm = normalizedStrings.value
        val normValues = fields.map {
          case (fieldType, index) =>
            norm.get(index).map {
              f =>
                (f._1, f._2.apply(row(index)), f._3.apply(row(index)))
            }.getOrElse(
                throw new UnexpectedValueException(s"Unexpected String Value exception ${row(index)}"))
        }

        val (_, indexes, values) = normValues.tail.scanLeft((normValues.head))((b, a) => (b._1 + a._1, (b._1 + a._2), a._3)).filter(_._3 != 0).unzip3
        val rowIndexD = rowIndex.toDouble +1
        (idField.head._1.dataType) match {
          case (StringType) => {
            dataSetType match {
              case DataSetType.Test => (Map( rowIndexD-> row(target.head)), LabeledPoint(rowIndexD, Vectors.sparse(columnsSize, indexes.toArray, values.toArray)))
              case DataSetType.Train => (Map(rowIndexD -> row(target.head)), LabeledPoint(rowIndexD,Vectors.sparse(columnsSize, indexes.toArray, values.toArray)))
            }
          }
          case _ => {
            dataSetType match {
              case DataSetType.Train => (Map(rowIndexD -> row(target.head) ), LabeledPoint(toDouble(row(target.head)), Vectors.sparse(columnsSize, indexes.toArray, values.toArray)))
              case DataSetType.Test => (Map(rowIndexD -> row(target.head) ), LabeledPoint(rowIndexD, Vectors.sparse(columnsSize, indexes.toArray, values.toArray)))
            }
          }
        }
    }
  }

  def extractStringsFromTrainTestSchema(trainDataSet: Dataset, testDataSet: Dataset, target: Seq[Int]): Dataset = {
    val rdd = trainDataSet.slice(excludes = target)
    val trainTestRDD = rdd.union(testDataSet)
    trainTestRDD.name = trainDataSet.name
    val trainTestSchemaRDD: Dataset = trainDataSet.sqlContext.applySchema(trainTestRDD, StructType(testDataSet.schema.fields))
    trainTestSchemaRDD
  }

  def toDouble(toConvert: Any): Double = {
    toConvert match {
      case v: Int => v.toDouble
      case v: Long => v.toDouble
      case v: BigDecimal => v.toDouble
      case v: Double => v
      case v: Timestamp => (v.getTime / 3600000).toDouble
      case v: String => v.toDouble
      case v: Byte => v.toDouble
      case v: Boolean => v match {
        case true => 1d
        case false => 0d
      }
      case _ => throw new Exception(toConvert.toString)
    }
  }

  def mapStringIdsToInt(rdd: SchemaRDD, columns: Seq[String]): Seq[Int] = rdd.schema.fields.zipWithIndex.
    filter(f => columns.contains(f._1.name)).map(_._2)


  def mapIdsToInt(rdd: SchemaRDD, columns: Seq[Union[IS]]): Seq[Int] = {
    columns.headOption.map {
      _.value[Int].map {
        _ => columns.map(_.value[Int].get)
      }.getOrElse {
        mapStringIdsToInt(rdd, columns.map(_.value[String].get))
      }
    }.getOrElse(Seq.empty[Int])
  }
}

