package org.apache.flink;

/**
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

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.Serializable;
import java.util.Collection;

/**
 * K-Means clustering in Flink with data from MongoDB
 */
public class Job {

	public static DataSet<Tuple2<BSONWritable, BSONWritable>> readFromMongo(ExecutionEnvironment env, String uri) {
		JobConf conf = new JobConf();
		conf.set("mongo.input.uri", uri);
		MongoInputFormat mongoInputFormat = new MongoInputFormat();
		return env.createHadoopInput(mongoInputFormat, BSONWritable.class, BSONWritable.class, conf);
	}

	public static void writeToMongo(DataSet<Tuple2<BSONWritable, BSONWritable>> result, String uri) {
		JobConf conf = new JobConf();
		conf.set("mongo.output.uri", uri);
		MongoOutputFormat<BSONWritable, BSONWritable> mongoOutputFormat = new MongoOutputFormat<BSONWritable, BSONWritable>();
		result.output(new HadoopOutputFormat<BSONWritable, BSONWritable>(mongoOutputFormat, conf));
	}

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


		DataSet<Tuple2<BSONWritable, BSONWritable>> inPoints = readFromMongo(env, "mongodb://localhost:27017/points");
		DataSet<Tuple2<BSONWritable, BSONWritable>> inCenters = readFromMongo(env, "mongodb://localhost:27017/centers");


		// get input data
		DataSet<Point> points = convertToPointSet(inPoints);
		DataSet<Centroid> centroids = convertToCentroidSet(inCenters);

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroids.iterate(10);

		DataSet<Centroid> newCentroids = points
				// compute closest centroid for each point
				.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
						// count and sum point coordinates for each centroid
				.map(new CountAppender())
				.groupBy(0).reduce(new CentroidAccumulator())
						// compute new centroids from point counts and coordinate sums
				.map(new CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple2<Integer, Point>> clusteredPoints = points
				// assign points to final clusters
				.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

		DataSet<Tuple2<BSONWritable, BSONWritable>> mongoResult = convertResultToBSON(clusteredPoints);
		writeToMongo(mongoResult, "mongodb://localhost:27017/result");

		// execute program
		env.execute("KMeans Example");
	}

	public static DataSet<Tuple2<BSONWritable, BSONWritable>> convertResultToBSON(DataSet<Tuple2<Integer, Point>> in) {
		return in.map(new MapFunction<Tuple2<Integer, Point>, Tuple2<BSONWritable, BSONWritable>>() {
			@Override
			public Tuple2<BSONWritable, BSONWritable> map(Tuple2<Integer, Point> integerPointTuple2) throws Exception {
				return new Tuple2<BSONWritable, BSONWritable>(null, null); /* TODO */
			}
		});
	}

	public static DataSet<Point> convertToPointSet(DataSet<Tuple2<BSONWritable, BSONWritable>> in) {
		return in.map(new MapFunction<Tuple2<BSONWritable, BSONWritable>, Point>() {
			@Override
			public Point map(Tuple2<BSONWritable, BSONWritable> bsonWritableBSONWritableTuple2) throws Exception {
				return new Point( /* TODO */);
			}
		});
	}

	public static DataSet<Centroid> convertToCentroidSet(DataSet<Tuple2<BSONWritable, BSONWritable>> in) {
		return in.map(new MapFunction<Tuple2<BSONWritable, BSONWritable>, Centroid>() {
			@Override
			public Centroid map(Tuple2<BSONWritable, BSONWritable> bsonWritableBSONWritableTuple2) throws Exception {
				return new Centroid( /* TODO */);
			}
		});
	}


	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * A simple two-dimensional point.
	 */
	public static class Point implements Serializable {

		public double x, y;

		public Point() {}

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public Point add(Point other) {
			x += other.x;
			y += other.y;
			return this;
		}

		public Point div(long val) {
			x /= val;
			y /= val;
			return this;
		}

		public double euclideanDistance(Point other) {
			return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y));
		}

		public void clear() {
			x = y = 0.0;
		}

		@Override
		public String toString() {
			return x + " " + y;
		}
	}

	/**
	 * A simple two-dimensional centroid, basically a point with an ID.
	 */
	public static class Centroid extends Point {

		public int id;

		public Centroid() {}

		public Centroid(int id, double x, double y) {
			super(x,y);
			this.id = id;
		}

		public Centroid(int id, Point p) {
			super(p.x, p.y);
			this.id = id;
		}

		@Override
		public String toString() {
			return id + " " + super.toString();
		}
	}


	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/** Converts a Tuple2<Double,Double> into a Point. */
	@FunctionAnnotation.ForwardedFields("0->x; 1->y")
	public static final class TuplePointConverter implements MapFunction<Tuple2<Double, Double>, Point> {

		@Override
		public Point map(Tuple2<Double, Double> t) throws Exception {
			return new Point(t.f0, t.f1);
		}
	}

	/** Converts a Tuple3<Integer, Double,Double> into a Centroid. */
	@FunctionAnnotation.ForwardedFields("0->id; 1->x; 2->y")
	public static final class TupleCentroidConverter implements MapFunction<Tuple3<Integer, Double, Double>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Double, Double> t) throws Exception {
			return new Centroid(t.f0, t.f1, t.f2);
		}
	}

	/** Determines the closest cluster center for a data point. */
	@FunctionAnnotation.ForwardedFields("*->1")
	public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
		private Collection<Centroid> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, Point> map(Point p) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Centroid centroid : centroids) {
				// compute distance
				double distance = p.euclideanDistance(centroid);

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.id;
				}
			}

			// emit a new record with the center id and the data point.
			return new Tuple2<Integer, Point>(closestCentroidId, p);
		}
	}

	/** Appends a count variable to the tuple. */
	@FunctionAnnotation.ForwardedFields("f0;f1")
	public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
			return new Tuple3<Integer, Point, Long>(t.f0, t.f1, 1L);
		}
	}

	/** Sums and counts point coordinates. */
	@FunctionAnnotation.ForwardedFields("0")
	public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			return new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	@FunctionAnnotation.ForwardedFields("0->id")
	public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			return new Centroid(value.f0, value.f1.div(value.f2));
		}
	}
}
