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
package org.lance.spark.utils;

import java.util.function.Supplier;

/**
 * Memoizing single-value supplier. Factory runs at most once on success; a throwing factory leaves
 * the holder uninitialized so the next caller can retry. {@link #isInitialized()} and {@link
 * #getIfInitialized()} let owners decide whether close-time cleanup is needed without forcing
 * initialization.
 */
public final class LazyResource<T> implements Supplier<T> {
  private final Supplier<T> factory;
  private volatile boolean initialized;
  private T value;

  public LazyResource(Supplier<T> factory) {
    if (factory == null) {
      throw new NullPointerException("factory");
    }
    this.factory = factory;
  }

  @Override
  public T get() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          T produced = factory.get();
          value = produced;
          initialized = true;
        }
      }
    }
    return value;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public T getIfInitialized() {
    return initialized ? value : null;
  }
}
