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
package eleflow.sparknotebook.visualization

import eleflow.sparknotebook.data.{FileDataset, Dataset}
import org.apache.spark.sql.SchemaRDD
import org.refptr.iscala.display.HTMLDisplay

import scalatags.Text.TypedTag
import scalatags.Text.all._
import Dataset._


/**
 * SparkNotebook
 * Copyright (C) 2014 eleflow.
 * User: paulomagalhaes
 * Date: 10/27/14 10:24 AM
 */
object RichDisplay {



  implicit val HTMLTypedTag = HTMLDisplay[TypedTag[String]](_.toString)
  implicit val HTMLSchemaRdd = HTMLDisplay[SchemaRDD, TypedTag[String]] { rdd:SchemaRDD =>

    div(style:="overflow:scroll", table(
      tr(
        rdd.schema.fieldNames.map(column=>th(column))
      ),
      rdd.take(7).map(row=>
        tr(row.map(field=> td(String.valueOf(field)))
        ))
    ))
  }


  implicit val HTMLSeqAny = HTMLDisplay[Seq[Any], TypedTag[String]] { seq =>
    div(style:="overflow:scroll", table(
      seq.zipWithIndex.map(row=>
        tr(th(row._2), td(String.valueOf(row._1))))
    ))
  }

  implicit val HTMLSeqTuples = HTMLDisplay[Seq[Product], TypedTag[String]] { seq =>
    div(style:="overflow:scroll", table(
      seq.zipWithIndex.map(row=>
        tr(th(row._2), row._1.productIterator.toList.map(field=> td(String.valueOf(field)))
        ))
    ))
  }


  implicit val HTMLMapAny = HTMLDisplay[Map[Any, Any], TypedTag[String]] { aMap =>

    div(style:="overflow:scroll", table(
      aMap.map(entry=>
        tr(td(String.valueOf(entry._1)), td(String.valueOf(entry._2))
        )).toSeq
    ))
  }

  implicit val HTMLArrayTuples = HTMLDisplay[Array[Product], Seq[Product]] { array =>
    array.toSeq
  }

  implicit val HTMLDataset = HTMLDisplay[Dataset, SchemaRDD] { dataset =>
    dataset
  }

  implicit val HTMLFileDataset = HTMLDisplay[FileDataset, SchemaRDD] { dataset =>
    dataset
  }
}
