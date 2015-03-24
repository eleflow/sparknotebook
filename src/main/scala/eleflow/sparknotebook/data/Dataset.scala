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

import java.net.URI
import java.sql.Timestamp
import eleflow.sparknotebook.enums.DataSetType
import eleflow.sparknotebook.{SparkNotebookContext, ClusterSettings}
import eleflow.sparknotebook.exception.{InvalidDataException, UnexpectedFileFormatException}
import eleflow.sparknotebook.util.DateTimeParser
import eleflow.sparknotebook.enums.DateSplitType._
import eleflow.sparknotebook.enums.DataSetType
import eleflow.sparknotebook.exception.InvalidDataException
import eleflow.sparknotebook.SparkNotebookContext
import eleflow.sparknotebook.util.DateTimeParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.types.{DataType, StructField}
import org.joda.time.{DateTime, DateTimeZone, Days}

import scala.collection.immutable.TreeSet


/**
 * SparkNotebook
 * Copyright (C) 2014 eleflow.
 * User: paulomagalhaes
 * Date: 11/4/14 3:44 PM
 */

object Dataset {
  implicit def DatasetToSchemaRdd(dataset: Dataset): SchemaRDD = dataset.toSchemaRDD()

  implicit def SchemaRddToDataset(schemaRdd: SchemaRDD): Dataset = new Dataset(schemaRdd)

  implicit def FileDatasetToDataset(fileDS: FileDataset): Dataset = new Dataset(fileDS.toSchemaRDD)

  implicit def FileDatasetToSchemaRdd(fileDS: FileDataset): SchemaRDD = fileDS.toSchemaRDD

  def apply(uc: SparkNotebookContext, file: String, separator: String = ",") = {
    new FileDataset(uc, file, separator)
  }
}

class Dataset private[data](schemaRdd: SchemaRDD, originalDataset: Option[Dataset] = None, defaultSummarizedColumns: Option[RDD[(Int, (Int, (Any) => Int, (Any) => Double))]] = None) extends Serializable {
  originalDataset.map(f => nameSchemaRDD(f.toSchemaRDD)).getOrElse(nameSchemaRDD(schemaRdd)).map(schemaRdd.registerTempTable(_))

  private def nameSchemaRDD(schemaRDD: SchemaRDD) = {
    schemaRDD.name match {
      case null => None
      case _ => Some(schemaRdd.name)
    }
  }

  import org.apache.spark.SparkContext._

  lazy val columnsSize = summarizedColumns.map(_._2._1).sum().toInt

  lazy val summarizedColumns = defaultSummarizedColumns.getOrElse(summarizeColumns.setName("summarizedColumns").cache())

  lazy val columnIndexOf = this.schema.fieldNames.zipWithIndex.toSet.toMap

  private def summarizeColumns = {

    val fieldsTuple = schemaRdd.schema.fields.zipWithIndex.partition(f => f._1.dataType == StringType)
    val (stringFields, nonStringFields) = (fieldsTuple._1.map(_._2), fieldsTuple._2.map(_._2))
    val valuex = schemaRdd.flatMap {
      row =>
        stringFields.map {
          sf =>
            (sf, TreeSet(row.getString(sf)))
        }
    }.reduceByKey(_ ++ _)
    val stringFieldsRdd: RDD[(Int, (Int, (Any => Int), (Any => Double)))] = valuex.map {
      case (index, values) => (index ->(values.size, values.zipWithIndex.map(f => (f._1, f._2)).toMap, ((_: Any) => 1.0)))
    }
    val nonStringMap: Seq[(Int, (Int, (Any => Int), (Any => Double)))] = nonStringFields.map { f => (f, (1, ((_: Any) => 0), ((DataTransformer.toDouble _))))}
    stringFieldsRdd.union(stringFieldsRdd.context.parallelize(nonStringMap))
  }

  lazy val summarizedColumnsIndex = summarizeColumnsIndex

  private def summarizeColumnsIndex = {
    val summarized = summarizedColumns.sortBy(_._1).map {
      f =>
        f._2._2 match {
          case m: Map[Any, Int] => (f._1, m.map(value => value._2 ->(this.schema.fieldNames(f._1), value._1.toString)))
          case _: (Any => Int) => (f._1, Map(0 ->(this.schema.fieldNames(f._1), "")))
        }
    }.collect
    summarized.foldLeft(Map.empty[Int, (String, String)])((b, a) =>
      (b ++ a._2.map(f => (f._1 + b.size -> f._2))))
  }

  var labels = Seq(0)

  def applyColumnNames(columnNames: Seq[String]) = {
    val structFields = schemaRdd.schema.fields.zip(columnNames).map {
      case (structField, columnName) =>
        new StructField(columnName, structField.dataType, structField.nullable)
    }
    val newSchemaStruct = StructType(structFields)
    val newSchemaRDD = schemaRdd.sqlContext.applySchema(schemaRdd, newSchemaStruct)
    newSchemaRDD.name = this.name
    new Dataset(newSchemaRDD, Some(this))
  }

  def applyColumnTypes(columnTypes: Seq[DataType]) = {
    val structFields = schemaRdd.schema.fields.zip(columnTypes).map {
      case (structField, dataType) =>
        new StructField(structField.name, dataType, structField.nullable)
    }
    val newSchema = StructType(structFields)
    val newRowRDD = convert(schemaRdd, newSchema)

    val newSchemaRDD = schemaRdd.sqlContext.applySchema(newRowRDD, newSchema)
    newSchemaRDD.name = this.name
    new Dataset(newSchemaRDD, Some(this))
  }

  def columnTypes(): Seq[DataType] = {
    schemaRdd.schema.fields.map(_.dataType)
  }

  def columnNames(): Seq[String] = {
    schemaRdd.schema.fields.map(_.name)
  }

  private def convert(rowRdd: RDD[Row], newSchema: StructType): RDD[Row] = {
    rowRdd.map { row =>
      val values = row.zip(newSchema.fields).map {
        case (null, _) => null
        case (value: Double, StructField(_, DoubleType, _, _)) => value
        case (value: BigDecimal, StructField(_, DecimalType(), _, _)) => value
        case (value: Timestamp, StructField(_, TimestampType, _, _)) => value
        case (value: Long, StructField(_, LongType, _, _)) => value
        case (value: Int, StructField(_, IntegerType, _, _)) => value
        case (value: Short, StructField(_, ShortType, _, _)) => value
        case (value: Boolean, StructField(_, BooleanType, _, _)) => value
        case (value, StructField(_, DecimalType(), _, _)) => BigDecimal(value.toString)
        case (value, StructField(_, DoubleType, _, _)) => value.toString.toDouble
        case (value, StructField(_, LongType, _, _)) => value.toString.toLong
        case (value, StructField(_, IntegerType, _, _)) => value.toString.toInt
        case (value, StructField(_, ShortType, _, _)) => value.toString.toShort
        //converter de double
        case (value, StructField(_, BooleanType, _, _)) => value.toString match {
          case "1" | "t" | "true" => true
          case "0" | "f" | "false" => false
          case a => throw new InvalidDataException(s"$a is an invalid Boolean value")
        }
        case (value, StructField(_, TimestampType, _, _)) => new Timestamp(DateTimeParser.parse(value.toString).map(_.toDate.getTime).getOrElse(throw new InvalidDataException("Unsupported data format Exception, please specify the date format")))
        case (value, StructField(_, StringType, _, _)) => value.toString
      }
      Row(values: _*)
    }
  }

  def sliceByName(includes: Seq[String] = (schemaRdd.schema.fields.map(_.name)), excludes: Seq[String] = Seq[String]()): Dataset = {
    val includesIndices = schemaRdd.schema.fields.zipWithIndex.collect {
      case (structField, index) if (includes.contains(structField.name) && !excludes.contains(structField.name)) => index
    }
    slice(includesIndices, Seq[Int]())
  }

  def slice(includes: Seq[Int] = (0 to schemaRdd.schema.fields.size), excludes: Seq[Int] = Seq.empty[Int]): Dataset = {

    val fields = schemaRdd.schema.fields.zipWithIndex.collect {
      case (structField, index) if (includes.contains(index) && !excludes.contains(index)) => structField.name;
    }
    import schemaRdd.sqlContext.symbolToUnresolvedAttribute
    val filtered = fields.map(x => symbolToUnresolvedAttribute(Symbol(x)))
    val newSchemaRdd = schemaRdd.select(filtered: _*)
    new Dataset(newSchemaRdd, None)
  }

  def toSchemaRDD(): SchemaRDD = schemaRdd

  def toLabeledPoint = {
    DataTransformer.createLabeledPointFromRDD(schemaRdd, labels, summarizedColumns, DataSetType.Test, columnsSize - 1).values
  }

  def formatDateValues(index: Int, dateSplitter: Long): SchemaRDD = {
    val rdd = schemaRdd.map { f =>
      val (before, after) = f.toSeq.splitAt(index)
      val formattedDate = splitDateValues(f, index, dateSplitter)
      Row(before ++ formattedDate ++ after.headOption.map(_ => after.tail).getOrElse(Seq.empty): _*)
    }
    val (beforeFields, afterFields) = schemaRdd.schema.fields.splitAt(index)
    val dateFields = (1 to determineSizeOfSplitter(dateSplitter)).map(index => new StructField(afterFields.head.name + index, IntegerType, false))
    val fields = beforeFields ++ dateFields ++ afterFields.headOption.map(_ => afterFields.tail).getOrElse(Seq.empty)
    val newSchema = StructType(fields)
    val newRowRDD = convert(rdd, newSchema)

    val newSchemaRDD = schemaRdd.sqlContext.applySchema(newRowRDD, newSchema)
    newSchemaRDD.name = this.name
    new Dataset(newSchemaRDD, Some(this))
  }

  type DateSplitterColumnSize = (Long, Long, Int) => Int
  type NoSplitterColumnSize = (Long, Int) => Int

  private def splitVerifier: DateSplitterColumnSize = (dateSplitter: Long, verifier: Long, value: Int) =>
    if (contains(dateSplitter, verifier)) {
      value + 1
    } else value

  private def noSplit: NoSplitterColumnSize = (dateSplitter: Long, value: Int) =>
    if (contains(dateSplitter, NoSplit)) {
      0
    } else value

  private def determineSizeOfSplitter(dateSplitter: Long) =
    splitVerifier(dateSplitter, Period,
      splitVerifier(dateSplitter, DayOfAWeek,
        noSplit(dateSplitter, 0)
      )
    )


  val dayZero = new DateTime(1970, 1, 1, 0, 0, 0)

  type DateTimeToInt = DateTime => Int

  type RowDateSplitter = (Long, DateTimeToInt, Seq[Int]) => Seq[Int]

  val daysBetween: DateTimeToInt = {
    case d: DateTime => Days.daysBetween(dayZero, d).getDays
  }
  val getDayOfAWeek: DateTimeToInt = {
    case d: DateTime => d.getDayOfWeek
  }
  val period: DateTimeToInt = {
    case d: DateTime => DateTimeParser.period(d).id
  }

  protected def splitDateValues(line: Row, index: Int, dateSplitter: Long) = {
    def splitDateValues: RowDateSplitter = {
      (verifier: Long, datetimefunc: DateTimeToInt, seq: Seq[Int]) =>
        if (contains(dateSplitter, verifier)) {
          val dateValue = if (line.isNullAt(index)) dayZero else new DateTime(line(index).asInstanceOf[Timestamp].getTime, DateTimeZone.UTC)
          seq ++ Seq(datetimefunc(dateValue))
        } else seq
    }
    splitDateValues(Period, period, splitDateValues(DayOfAWeek, getDayOfAWeek, splitDateValues(NoSplit, daysBetween, Seq.empty[Int])))
  }




  def translateCorrelation(array: Array[(Double, Int)]) = {
    array.map {
      f => summarizedColumns.map {
        g => g
      }
    }
  }
}

class FileDataset protected[data](@transient uc: SparkNotebookContext, file: String, separator: String = ",", header: Option[String] = None) extends Serializable {

  lazy val numberOfPartitions = 4 * (ClusterSettings.getNumberOfCores)

  lazy val columnTypes: Array[DataType] = typeLine.map(dataType)

  lazy val typeLine: Array[String] = extractFirstCompleteLine(originalRdd)

  lazy val columnNames: Array[String] = headerOrFirstLine().split(separator, -1)

  lazy val firstLine: String = loadedRDD.first

  lazy val loadedRDD = {
    println(s"localFileName:$localFileName")
    val file = uc.sparkContext.textFile(localFileName)
    println("file")
    file
  }

  lazy val localFileName: String = {
    uc.sparkContext() // make sure that the cluster is up
    val uri = Some(new URI(file))
    val destURI = uri.filter { f => f.getScheme() != null && f.getScheme().startsWith("s3")}.map { vl =>
      val destURI = s"hdfs:///tmp${vl.getPath()}"
      uc.copy(file, destURI)
      destURI
    }.getOrElse(file)
    destURI
  }

  lazy val originalRdd: RDD[Array[String]] = initOriginalRdd(headerOrFirstLine(), localFileName)

  def headerOrFirstLine(): String = {
    header.getOrElse(firstLine)
  }

  def initOriginalRdd(header: String, rdd: RDD[String]): RDD[Array[String]] = {
    val localHeader = header
    val oRdd = rdd.filter(line => line != localHeader).map(_.split(separator, -1))
    oRdd.setName(localFileName)
    oRdd.cache

  }

  def initOriginalRdd(header: String, localFileName: String): RDD[Array[String]] = {
    initOriginalRdd(header, loadedRDD)
  }

  private def dataType(data: String): DataType = {
    val double = """[+-]?\d*\.?\d*E?\d{1,4}"""
    val intNumber = "-?\\d{1,9}" // more then 9 it cannot be int
    val longNumber = "-?\\d{10,18}" // more then 19 it cannot be long
    if (data.matches(intNumber))
      LongType // TODO: To return IntType the whole data set (or sample) needs to be analyzed.
    else if (data.matches(longNumber))
      LongType
    else if (data.matches(double))
      DecimalType()
    else
      parse(data).getOrElse(StringType)
  }

  protected def parse(data: String): Option[DataType] = DateTimeParser.isValidDate(data) match {
    case true => Some(TimestampType)
    case false => None
  }

  lazy val schemaRDD: SchemaRDD = initSchemaRDD(columnNames, originalRdd, structType)


  protected def initSchemaRDD(columnNames: Array[String], originalRdd: RDD[Array[String]], structType: StructType): SchemaRDD = {

    val sqlContext = uc.sqlContext
    val colNames = columnNames
    val rowRdd = originalRdd.map { colValues =>
      if (colValues.size != colNames.size) throw new UnexpectedFileFormatException(s"Files should have the same number of columns. Line ${colValues.mkString(",")} \n has #${colValues.size} and Header have #${colNames.size}")
      val columns = colValues.zip(structType.fields).zipWithIndex.map { case ((value, tp), index) =>
        //TODO não converter a data aqui
        tp.dataType match {
          case DecimalType() | DoubleType => value.headOption.map(f => BigDecimal(value.trim)).getOrElse(throw new UnexpectedFileFormatException(s"Numeric columns can't be empty.\nIndex $index is empty at: ${colValues.mkString(",")}"))
          case LongType => value.headOption.map(f => value.trim.toLong).getOrElse(throw new UnexpectedFileFormatException(s"Long Numeric columns can't be empty.\nIndex $index is empty at: ${colValues.mkString(",")}"))
          case IntegerType => value.headOption.map(f => value.trim.toInt).getOrElse(throw new UnexpectedFileFormatException(s"Int Numeric columns can't be empty.\nIndex $index is empty at: ${colValues.mkString(",")}"))
          case TimestampType => new Timestamp(DateTimeParser.parse(value).map(_.toDate.getTime).getOrElse(0))
          case _ => if (value.trim.isEmpty) "0" else value
        }
      }
      Row(columns: _*)
    }
    val schema = sqlContext.applySchema(rowRdd, structType)
    val tableName = extractTableName(file)
    schema.name = tableName
    schema.registerTempTable(tableName)
    schema.repartition(numberOfPartitions)
    schema
  }

  protected def structType(): StructType = {
    if (columnNames.size != typeLine.size || columnNames.size == 0) StructType(List.empty[StructField])
    else {
      val fields = columnNames.zip(columnTypes).map { case (columnName, columnType) => new StructField(columnName, columnType, true)}
      StructType(fields)
    }
  }

  protected def extractFirstCompleteLine(dataRdd: RDD[Array[String]]): Array[String] = {
    val x = dataRdd.filter { f =>
      !f.isEmpty &&
        f.forall(!_.isEmpty)
    }.first
    x
  }

  protected def extractTableName(file: String): String = {
    val name = file.split("/").last
    val index = name.indexOf(".csv") + name.indexOf(".txt")
    name.splitAt(index + 1).productIterator.toList.filter(!_.toString.isEmpty).head.toString
  }

  def header(newHeader: String) = {
    new FileDataset(uc, file, separator, Some(newHeader))
  }

  def toSchemaRDD = schemaRDD

  def toDataset(): Dataset = {
    new Dataset(schemaRDD)
  }
}
