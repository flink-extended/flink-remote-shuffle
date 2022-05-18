/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.core.listener.FailureListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** A {@link FailureListener} implementation for tests. */
public class TestFailureListener implements FailureListener {

    private final CompletableFuture<Throwable> throwable = new CompletableFuture<>();

    @Override
    public void notifyFailure(Throwable throwable) {
        this.throwable.complete(throwable);
    }

    public boolean isFailed() {
        return throwable.isDone();
    }

    public Throwable getFailure() throws ExecutionException, InterruptedException {
        return throwable.get();
    }
}
