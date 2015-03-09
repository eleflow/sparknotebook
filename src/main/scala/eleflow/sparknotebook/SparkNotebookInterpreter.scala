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

import org.apache.spark.SparkConf
import org.apache.spark.repl.SparkILoop

import scala.collection.immutable
import scala.tools.nsc.interpreter.{IR, NamedParam}
import org.apache.spark.repl.SparkILoop
import org.apache.spark.{SparkConf}
import org.refptr.iscala.{Results, Interpreter}


class SparkNotebookInterpreter(classpath: String, args: Seq[String], usejavacp: Boolean=true) extends Interpreter(classpath, args, false, usejavacp) {

  var snc: SparkNotebookContext = _

  override  def initializeSpark() {
    snc = createContext()

    val namedParam = NamedParam[SparkNotebookContext]("snc", snc)
    intp.beQuietDuring(bind(namedParam.name, namedParam.tpe, namedParam.value, immutable.List("@transient"))) match {
      case IR.Success => Unit
      case _ => throw new RuntimeException("Spark failed to initialize")
    }

    val importSVresult = interpret( """
import org.apache.spark.SparkContext._
import eleflow.sparknotebook._
import eleflow.sparknotebook.visualization.RichDisplay._
import snc._
                                    """)
    importSVresult match {
      case Results.Value(value, tpe, repr) => Unit
      case Results.NoValue => Unit
      case Results.Exception(_,_,_,ee) => throw new RuntimeException("SparkContext failed to be imported", ee)
      case _ => throw new RuntimeException("SparkContext failed to be imported")
    }

  }

  override  def sparkCleanUp() {
    if (snc!=null) {
      snc.clearContext
    }
  }

  override lazy val appName: String = "SparkNotebook"

  def createContext(): SparkNotebookContext = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    val jars = SparkILoop.getAddedJars
    val conf = new SparkConf()
      .setMaster(getMaster())
      .setAppName(this.appName)
      .setJars(jars)
      .set("spark.repl.class.uri", intp.classServer.uri) //very important! spark treat REPL very differently
    .set("spark.files.overwrite","true")
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }
    new SparkNotebookContext(conf)
  }

  protected def getMaster(): String = {
    val master = {
      val envMaster = sys.env.get("MASTER")
      val propMaster = sys.props.get("spark.master")
      propMaster.orElse(envMaster).getOrElse("local[*]")
    }
    master
  }

}