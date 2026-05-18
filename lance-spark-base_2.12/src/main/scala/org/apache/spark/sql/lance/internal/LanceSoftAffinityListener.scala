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
package org.apache.spark.sql.lance.internal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.lance.spark.internal.LanceSoftAffinityManager

import scala.util.control.NonFatal

/**
 * SparkListener that tracks executor lifecycle and updates the consistent hash ring in
 * [[LanceSoftAffinityManager]]. Registered once per SparkContext.
 */
class LanceSoftAffinityListener extends SparkListener with Logging {

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val host = event.executorInfo.executorHost
    val executorId = event.executorId
    LanceSoftAffinityManager.getInstance().onExecutorAdded(executorId, host)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    val executorId = event.executorId
    // Host not available in SparkListenerExecutorRemoved; use empty string as placeholder.
    // removeNode matches by ExecutorNode.equals which uses both id and host, so we need
    // to track the mapping. For simplicity, iterate the ring to find and remove by id.
    LanceSoftAffinityManager.getInstance().onExecutorRemovedById(executorId)
  }
}

object LanceSoftAffinityListener extends Logging {

  @volatile private var registered: Boolean = false

  def ensureRegistered(sc: SparkContext): Unit = {
    if (registered) return
    synchronized {
      if (registered) return
      try {
        // Seed the ring with already-active executors BEFORE registering the listener
        // to avoid a race where onExecutorRemoved fires for an executor not yet seeded.
        val statusStore = sc.statusStore
        statusStore.executorList(true).foreach { exec =>
          if (exec.id != "driver") {
            val host = extractHost(exec.hostPort)
            LanceSoftAffinityManager.getInstance().onExecutorAdded(exec.id, host)
            logInfo(s"LanceSoftAffinityListener registered ${exec.id}")
          }
        }
        sc.addSparkListener(new LanceSoftAffinityListener())
        registered = true
        logInfo("LanceSoftAffinityListener registered finish")
      } catch {
        case NonFatal(t) =>
          logWarning(s"Failed to register LanceSoftAffinityListener: ${t.getMessage}")
      }
    }
  }

  private def extractHost(hostPort: String): String = {
    if (hostPort == null || hostPort.isEmpty) return "unknown"
    val lastColon = hostPort.lastIndexOf(':')
    if (lastColon <= 0) hostPort else hostPort.substring(0, lastColon)
  }

  private[internal] def resetForTesting(): Unit = synchronized {
    registered = false
  }
}
