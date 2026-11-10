/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A drop bubbles through every drop target above where it landed. A target that takes the files
 * itself marks the native event, and the targets above skip the upload but still run their own
 * cleanup, so no target has to stop the event to avoid a double upload.
 */
const handled = new WeakSet<Event>();

export function markDropHandled(e: { nativeEvent: Event }): void {
  handled.add(e.nativeEvent);
}

export function isDropHandled(e: { nativeEvent: Event }): boolean {
  return handled.has(e.nativeEvent);
}
