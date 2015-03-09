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
package eleflow.sparknotebook

import eleflow.sparknotebook.data.{DataTransformer, Dataset}
import eleflow.sparknotebook.enums.DataSetType
import org.apache.spark.SparkException
import org.scalatest._
import org.scalatest.mock.MockitoSugar

/**
 * Created by dirceu on 14/10/14.
 */
class FuncTestSparkNotebookContext extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterWithContext {
  this: Suite =>

  val uberContext = context

  "Functional SparkNotebookContext" should
    "correctly load rdd" in {

    import eleflow.sparknotebook.data.Dataset._

    val dataset = Dataset(context, s"${defaultFilePath}FuncTestSparkNotebookContextFile1.csv")

    val testDataSet = Dataset(context, s"${defaultFilePath}FuncTestSparkNotebookContextFile2.csv")

    val unionDataset= DataTransformer.extractStringsFromTrainTestSchema(dataset.toSchemaRDD, testDataSet.toSchemaRDD,
      Seq(0))
      val normalized = unionDataset.summarizedColumns.map {
      case (colIndex, (size, funcIndex, funcValue)) => (colIndex + 1, (size, funcIndex, funcValue))
    }
    val result = DataTransformer.createLabeledPointFromRDD(dataset, Seq(0), normalized,DataSetType.Train,unionDataset.columnsSize)
    val all = result.take(3)
    val (_, first) = all.head
    val (_, second) = all.tail.head
    assert(first.label == 1)
    assert(first.features.toArray.deep == Array[Double](5.0, 0.0, 1.0, 10.5).deep)
    assert(second.label == 2)
    assert(second.features.toArray.deep == Array[Double](1.0, 1.0, 0.0, 0.1).deep)
    context.clearContext
  }

  it should "Throw an exception when process an empty numeric column" in {

    @transient lazy val context = uberContext

    val sc = context.sparkContext
    try {
      import eleflow.sparknotebook.data.Dataset._
      val dataset = Dataset(context, s"${defaultFilePath}FuncTestSparkNotebookContextFile1.csv")
      dataset.take(3)
    } catch {
      case e: SparkException => {
        assert(e.getMessage.contains("UnexpectedFileFormatException"))
      }
    }
    context.clearContext
  }

  it should "Correct handle empty string values" in {
    @transient lazy val context = uberContext
    val sc = context.sparkContext
    val schemaRdd = Dataset(context, s"${defaultFilePath}FuncTestSparkNotebookContextEmpty.csv").schemaRDD
    val result = DataTransformer.createLabeledPointFromRDD(schemaRdd, Seq(0),DataSetType.Train)

    context.clearContext
  }

  it should "Throw an exception when input have different number of columns" in {
    val sc = context.sparkContext()
    try {

      val result = context.load(s"${defaultFilePath}FuncTestSparkNotebookContextFile1.csv", TestSparkConf.separator)
    } catch {
      case e: SparkException =>
        assert(e.getMessage.contains("UnexpectedFileFormatException"))
    }
    context.clearContext
  }

}
