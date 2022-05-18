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

package com.alibaba.flink.shuffle.yarn.utils;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Utils to deal with timeout cases for test. */
public class TestTimeoutUtils {
    public static <T> List<T> waitAllCompleted(
            List<CompletableFuture<T>> futuresList, long timeout, TimeUnit unit) throws Exception {
        CompletableFuture<Void> futureResult =
                CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[0]));
        futureResult.get(timeout, unit);
        return futuresList.stream()
                .filter(future -> future.isDone() && !future.isCompletedExceptionally())
                .map(CompletableFuture::join)
                .collect(Collectors.<T>toList());
    }

    @Test(expected = Exception.class)
    public void testTimeoutWorkNormal() throws Exception {
        CompletableFuture<Boolean> setupFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                Thread.sleep(2000);
                                return true;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            return false;
                        });
        waitAllCompleted(Collections.singletonList(setupFuture), 1, TimeUnit.SECONDS);
    }
}
