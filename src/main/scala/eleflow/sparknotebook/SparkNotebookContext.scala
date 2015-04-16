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

import java.io.{FileNotFoundException, InputStream, OutputStream}
import java.net.URI
import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectMetadata, PutObjectRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import eleflow.sparknotebook.data.Dataset

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileUtil, FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.Try
import scala.util.matching.Regex

object ClusterSettings {
  var kryoBufferMaxSize: Option[String] = None
  var maxResultSize = "2g"
  var masterInstanceType = "r3.large"
  var coreInstanceType = "r3.large"
  var coreInstanceCount = 3
  var spotPriceFactor: Option[String] = Some("1.3")
  var ec2KeyName: Option[String] = None
  var hadoopVersion = "2"
  var clusterName = "SparkNotebookCluster"
  var region: Option[String] = None
  var profile: Option[String] = None
  var resume = false
  var executorMemory: Option[String] = None
  var defaultParallelism: Option[Int] = None
  var master: Option[String] = None

  def slavesCores = ClusterSettings.coreInstanceType match {
    case s: String if s.endsWith("xlarge") => 4
    case s: String if s.endsWith("2xlarge") => 8
    case s: String if s.endsWith("4xlarge") => 16
    case s: String if s.endsWith("8xlarge") => 32
    case _ => 2
  }

  def getNumberOfCores = ClusterSettings.coreInstanceCount * slavesCores
}


/**
 * User: paulomagalhaes
 * Date: 8/15/14 12:24 PM
 */

class SparkNotebookContext(@transient sparkConf: SparkConf) extends Serializable with Logging {
  val version = SparkNotebookVersion.version

  protected def this(sparkConf: SparkConf, data: String) = this(sparkConf)

  @transient protected lazy val s3Client: AmazonS3 = new AmazonS3Client()
  @transient protected var sc: Option[SparkContext] = None
  @transient var _sqlContext: Option[HiveContext] = None
  private var _masterHost: Option[String] = None
  protected val basePath: String = "/"

  def sparkContext(): SparkContext = sc getOrElse {
    val context = if (ClusterSettings.master.isDefined) createSparkContextForProvisionedCluster(sparkConf)
    else createSparkContextForNewCluster(sparkConf)
    addClasspathToSparkContext(context)
    sc = Some(context)
    context
  }

  def addClasspathToSparkContext(context: SparkContext) {
    val jodaJar = "joda-time.joda-time-.*jar".r
    val sparkNotebookContextJar = "eleflow.sparknotebook-.*jar".r
    val guavaJar = "com.google.guava.*".r
    val mySqlDriver = "mysql-connector-java.*".r
    val urls = this.getClass().getClassLoader().asInstanceOf[java.net.URLClassLoader].getURLs
    val jarUrls = urls.filter(url =>
      jodaJar.findFirstIn(url.getFile) != None
        || sparkNotebookContextJar.findFirstIn(url.getFile) != None
        || guavaJar.findFirstIn(url.getFile) != None
        || mySqlDriver.findFirstIn(url.getFile) != None)
    jarUrls.foreach { url =>
      logInfo(s"adding ${url.getPath} to spark context jars")
      context.addJar(url.getPath)
    }
  }

  def masterHost(): String = {
    return _masterHost match {
      case Some(host) => host
      case None => {
        initHostNames
        _masterHost.get
      }
    }
  }

  def initHostNames {
    _masterHost = createCluster();
  }

  def masterHost_=(host: String): Unit = _masterHost = Some(host)

  def sqlContext(): HiveContext = {
    _sqlContext match {
      case None => {
        _sqlContext = Some(new HiveContext(sparkContext));
        HiveThriftServer2.startWithContext(_sqlContext.get)
        _sqlContext.get
      }
      case Some(ctx) => ctx
    }
  }

  def createSparkContextForNewCluster(conf: SparkConf): SparkContext = {
    log.info(s"connecting to $masterHost")
    conf.setMaster(s"spark://$masterHost:7077")
    confSetup(conf)

    return new SparkContext(conf)
  }

  private def confSetup(conf: SparkConf): Unit = {
    ClusterSettings.defaultParallelism.map(value => conf.set("spark.default.parallelism", value.toString))
    ClusterSettings.kryoBufferMaxSize.map(value => conf.set("spark.kryoserializer.buffer.max.mb", value.toString))

    conf.set("spark.driver.maxResultSize", ClusterSettings.maxResultSize)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    ClusterSettings.executorMemory.foreach(conf.set("spark.executor.memory", _))

    val defaultConfStream = this.getClass.getClassLoader.getResourceAsStream("spark-defaults.conf")
    if (defaultConfStream != null) {
      import scala.collection.JavaConversions._
      val defaultConf = IOUtils.readLines(defaultConfStream)
      defaultConf.map { line =>
        val keyValue = line.split("\\s+")
        if (keyValue.size == 2)
          conf.set(keyValue(0), keyValue(1))
      }
    }
    //according to keo, in Making Sense of Spark Performance webcast, this codec is better than default
    conf.set("spark.io.compression.codec","lzf")
    
    ClusterSettings.defaultParallelism.map(value => conf.set("spark.default.parallelism", value.toString))
    ClusterSettings.kryoBufferMaxSize.map(value => conf.set("spark.kryoserializer.buffer.max.mb", value.toString))

    conf.set("spark.driver.maxResultSize", ClusterSettings.maxResultSize)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    ClusterSettings.executorMemory.foreach(conf.set("spark.executor.memory", _))
    return new SparkContext(conf)
  }

  def createSparkContextForProvisionedCluster(conf: SparkConf): SparkContext = {
    log.info("connecting to localhost")
    conf.setMaster(ClusterSettings.master.get)
    confSetup(conf)
    new SparkContext(conf)
  }

  def shellRun(command: Seq[String]) = {
    val out = new StringBuilder

    val logger = ProcessLogger(
      (o: String) => {
        out.append(o); logInfo(o)
      },
      (e: String) => {
        println(e); logInfo(e)
      })
    command ! logger
    out.toString()
  }

  def createCluster(): Option[String] = {

    val path = getClass.getResource(s"${basePath}spark_ec2.py").getPath
    import ClusterSettings._
    val mandatory = Seq(path,
      "--hadoop-major-version", hadoopVersion,
      "--master-instance-type", masterInstanceType,
      "--slaves", coreInstanceCount.toString,
      "--instance-type", coreInstanceType)
    val command = mandatory ++ (ec2KeyName match {
      case None => Seq[String]()
      case Some(ec2KeyName) => Seq("--key-pair", ec2KeyName)
    })++ (spotPriceFactor match {
      case None => Seq[String]()
      case Some(spotPrice) => Seq("--spot-price", spotPrice.toString)
    }) ++ (region match {
      case None => Seq[String]()
      case Some(region) => Seq("--region", region.toString)
    }) ++ (profile match {
      case None => Seq[String]()
      case Some(profile) => Seq("--profile", profile.toString)
    }) ++ (if (resume) Seq("--resume") else Seq())

    val output = shellRun((command ++ Seq("launch", clusterName)))

    val pattern = new Regex("Spark standalone cluster started at http://([^:]+):8080")
    val host = pattern.findAllIn(output).matchData.map(_.group(1)).next
    return Some(host)
  }

  def terminate() {
    clearContext
    val path = getClass.getResource(s"${basePath}spark_ec2.py").getPath
    import ClusterSettings._

    val output = shellRun(Seq(path, "destroy", clusterName))
    _masterHost = None
    ClusterSettings.resume = false
  }

  def clusterInfo() {
    val path = getClass.getResource(s"${basePath}spark_ec2.py").getPath
    import ClusterSettings._
    val output = shellRun(Seq(path, "get-master", clusterName))
  }

  def clearContext {
    ClusterSettings.resume = true
    sc.map {
      f =>
        f.cancelAllJobs()
        f.stop()
    }
    _sqlContext = None
    sc = None
  }

  def reconnect(): Unit = {
    sc.map(_.stop())
    sc = None
    _sqlContext = None
  }
  def getAllFilesRecursively(path: Path): Seq[String] = {
    val fs = path.getFileSystem(new Configuration)
    @tailrec
    def iter(fs: FileSystem, paths: Seq[Path], result: Seq[String]): Seq[String] = paths match {
      case path :: tail =>
        val children: Seq[FileStatus] = try {
          fs.listStatus(path)
        } catch {
          case e: FileNotFoundException =>
            // listStatus throws FNFE if the dir is empty
            Seq.empty[FileStatus]
        }
        val (files, directories) = children.partition(_.isFile)
        iter(fs, tail ++ directories.map(_.getPath), files.map(_.getPath.toString) ++ result)
      case _ =>
        result
    }
    iter(fs, Seq(path), Seq())
  }

  def copyDir(input: String, output: String): Unit = {
    val from = createPathInstance(input)

    val files = getAllFilesRecursively(from)
    val to = output.replaceAll(new URI(input).getPath, "")
    copyDir(files, to)
  }

  def copyDir(inputFiles: Seq[String], output: String): Unit = {
    sparkContext.parallelize(inputFiles).foreach { inputFile =>
      val from = new URI(inputFile)

      copy(inputFile, s"$output/${from.getPath}")
    }
  }

  def copy(input: String, output: String): Unit = {
    val from = new URI(input)
    val to = new URI(output)
    val fromScheme = from.getScheme
    val toScheme = to.getScheme
    val conf = new Configuration()

    (fromScheme, toScheme) match {
      case ("s3n" | "s3", "s3n" | "s3") => ???
      case (fromAddr, _) if (fromAddr.startsWith("s3")) => {
        val outputPath = createPathInstance(output)
        val fs = createPathInstance(output).getFileSystem(conf)
        copyFromS3(from, outputPath, fs)
      }
      case _ => {
        val srcPath = createPathInstance(input)
        val srcFs = srcPath.getFileSystem(conf)
        val dstPath = createPathInstance(output)
        val dstFs = dstPath.getFileSystem(conf)
        FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf)
      }
    }
  }

  def fs(pathStr: String): FileSystem = {
    val path = createPathInstance(pathStr)
    path.getFileSystem(new Configuration)
  }

  def sql(sql: String) = {
    sqlContext().sql(sql)
  }

  protected def copyFromS3(input: URI, path: Path, fs: FileSystem): Unit = {
    val rangeObjectRequest: GetObjectRequest = new GetObjectRequest(input.getHost, input.getPath.substring(1))
    val inputStream: Try[InputStream] = Try {

      val objectPortion: S3Object = s3Client.getObject(rangeObjectRequest)
      objectPortion.getObjectContent()
    }
    inputStream.map {
      in =>
        val copyResult = Try(fs.create(path)).flatMap {
          out =>
            val copyResult = copyStreams(in, out)
            out.close
            copyResult
        }
        in.close
        copyResult
    }.recover {
      case e: Exception => throw e
    }
  }

  protected def createPathInstance(input: String) = new Path(input)

  protected def copyStreams(in: InputStream, out: OutputStream) = Try(IOUtils.copy(in, out))

  protected def copyToS3(input: Path, bucket: String, fileName: String): Unit = {

    val objRequest = new PutObjectRequest(bucket, fileName, readFromHDFS(input), new ObjectMetadata())
    s3Client.putObject(objRequest)
  }

  private def readFromHDFS(input: Path) = {
    val fs = input.getFileSystem(new Configuration)
    fs.open(input)
  }

  def load(file: String, separator: String = ",") = {
    Dataset(this, file, separator)
  }

}

