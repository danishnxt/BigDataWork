/**
  * Bespin: reference implementations of "big data" algorithms
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

package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

object ManualClockWrapper {
  def advanceManualClock(ssc: StreamingContext, timeToAdd: Long, sleepTime: Long = 0L): Unit = {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    clock.advance(timeToAdd)
    Thread.sleep(sleepTime)
  }
}
