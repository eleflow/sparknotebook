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
package eleflow.sparknotebook.util

import com.gensler.scalavro.util.Union
import com.gensler.scalavro.util.Union.union
/**
 * Created by dirceu on 03/11/14.
 */
object IntStringImplicitTypeConverter {

  type IS = union[Int]#or[String]
  implicit def convIntToUnion(i:Int): Union[IS] = {
    val union = new Union[IS]
      union.assign(i)
    union
  }
  implicit def convStringToUnion(i:String): Union[IS] = {
    val union = new Union[IS]
    union.assign(i)
    union
  }
}
