/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.utils;

import java.io.Serializable;
import java.util.Objects;

/** A tuple with 4 fields. */
public class Tuple4<T0, T1, T2, T3> implements Serializable {

    private static final long serialVersionUID = 453491702625489108L;

    /** Field 0 of the tuple. */
    public T0 f0;
    /** Field 1 of the tuple. */
    public T1 f1;
    /** Field 2 of the tuple. */
    public T2 f2;
    /** Field 3 of the tuple. */
    public T3 f3;

    public Tuple4(T0 f0, T1 f1, T2 f2, T3 f3) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }
        Tuple4<?, ?, ?, ?> tuple4 = (Tuple4<?, ?, ?, ?>) that;
        return Objects.equals(f0, tuple4.f0)
                && Objects.equals(f1, tuple4.f1)
                && Objects.equals(f2, tuple4.f2)
                && Objects.equals(f3, tuple4.f3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1, f2, f3);
    }

    public static <T0, T1, T2, T3> Tuple4<T0, T1, T2, T3> of(T0 f0, T1 f1, T2 f2, T3 f3) {
        return new Tuple4<>(f0, f1, f2, f3);
    }
}
