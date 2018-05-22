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

package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TaxiRideCleansing {

    private final static int maxDelay = 60;
    private final static int servingSpeed = 600;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("/Users/alex/study/flink/data/nycTaxiRides.gz",
                        maxDelay, servingSpeed));

        // Filter out all the rides that are do not begin or end in NYC
        SingleOutputStreamOperator<TaxiRide> nycRides = rides.filter(TaxiRideCleansing::nycFilter);
        nycRides.print();

        env.execute("Flink Batch Java API Skeleton");
    }

    private static boolean nycFilter(TaxiRide ride) {
        return GeoUtils.isInNYC(ride.endLon, ride.endLat)
                && GeoUtils.isInNYC(ride.startLon, ride.startLat);
    }
}
