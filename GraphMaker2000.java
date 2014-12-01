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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


@SuppressWarnings("serial")
public class GraphMaker2000 {

	public static void main(String[] args) throws Exception {

		// 1. set up the execution environment - typically done this way
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// 2. get input data - from file
		DataSet<String> text = env.readTextFile("file:///home/alex/Desktop/IMPRO/flinkxample/testfile0");		
		
		DataSet<String> edges =
				text
				.flatMap(new LineSplitter())	//splits String in lines
				.groupBy(0)
				.reduce(new Unique())			//cancels out all duplicate edges, should there be any
				.flatMap(new Sorter())			//sort edges alphabetically by vertex
				.groupBy(0)
				.aggregate(Aggregations.SUM, 1)	//count number of alphabetically sorted edges; can be two (bidirectional) or one (unidirectional)
				.filter(new EdgeFilter())		//allows only for edges with a count of >1 (i.e. 2), because they are mutual
				.flatMap(new BackToString()) 	//converts back to string; only "real", undirected edges from here
				;
		DataSet<Tuple2<String, String>> uniqueedges =
				edges
				.flatMap(new UniqueEdges());	//all edges unique, sorted alphabetically by vertex
		
		DataSet<Tuple2<String, String>> mirrorededges =
				edges
				.flatMap(new MirroredEdges());	//all edges mirrored (vertices swapped) 
				
		DataSet<Tuple2<String, String>> neighborlist =	
				mirrorededges					//uses mirrored edges so every vertex is captured
				.groupBy(0)
				.reduce(new GenerateNeighborList()) //outputs (vertex;{all neighbors})
				.filter(new NodeFilter(0.25)) //DEGREE DISTRIBUTION cancel some random neighbors by verge in brackets (for Math.random)
				;	
				
		DataSet<Tuple3<String, String, String>> firstjoin =
				uniqueedges
				.coGroup(neighborlist)			//uses neighbor list
				.where(0).equalTo(0)			//joins on start-V
				.with(new Linker1())			//outputs (start-V,end-V,neighbors of start-V)
				;
		
		DataSet<Tuple3<String, String, String>> secondjoin=
				firstjoin
				.coGroup(neighborlist).			//uses neighbor list
				where(1).equalTo(0).			//joins on end-V
				with(new Linker2())				//outputs (start-V,end-V,neighbors of start-V AND end-V)
				;
		DataSet<String> finalgraph=
				secondjoin
				.flatMap(new FinalList())		//outputs (start-V,end-V,count of duplicate neighbors)
				.filter(new WeightFilter())		//only allows for edges with weight >0
				.flatMap(new BackToString()) 	//converts back to string
				;
		//DEGREE DISTRIBUTION
//		DataSet<Tuple2<Integer,Integer>> degrees=
//				finalgraph
//				.flatMap(new NumberTurner())
//				.groupBy(0)
//				.aggregate(Aggregations.SUM, 1)
//				;
		
		// 4. specify where results go - in this case: write in file
		finalgraph.writeAsText("file:///home/alex/Desktop/IMPRO/flinkxample/testoutput");

		// 5. execute program
		env.execute("WordCount Example");
	}

	// 	User Functions

	public static final class FinalList implements FlatMapFunction<Tuple3<String,String,String>,Tuple2<String,Integer>>{
		
		public Integer countDuplicateNeighbors(Tuple3<String,String,String> s){
			int count=0;
			String[] tokens=s.f2.split("\\W+");
			Arrays.sort(tokens);
			for(int i=0; i<tokens.length-1;i++){
				if(tokens[i].equals(tokens[i+1])){count++;}
			}
			return count;
		}
		
		@Override
		public void flatMap(Tuple3<String, String, String> in,
				Collector<Tuple2<String,Integer>> out) throws Exception {
			
			out.collect(new Tuple2<String,Integer>(in.f0.concat(" ").concat(in.f1),countDuplicateNeighbors(in)));
			
		}

	
	}
	public static final class Linker1 implements CoGroupFunction<Tuple2<String, String>,Tuple2<String, String>,Tuple3<String, String, String>>{

		@Override
		public void coGroup(
				Iterable<Tuple2<String, String>> edges,
				Iterable<Tuple2<String, String>> neighbors,
				Collector<Tuple3<String, String, String>> out)
				throws Exception {
			
			Set<String> new_neighbor_set = new HashSet<String>(); //this way there is no double iteration
			for (Tuple2<String, String> val : neighbors) {
				new_neighbor_set.add(val.f1);
			}
			for(Tuple2<String, String> edge:edges){
				String vertex1 = edge.f0;
				String vertex2 = edge.f1;
				for(String new_neighbors:new_neighbor_set){
					out.collect(new Tuple3<String, String, String>(vertex1,vertex2,new_neighbors));
				}
			}
		}
	}
	public static final class Linker2 implements CoGroupFunction<Tuple3<String, String, String>,Tuple2<String, String>,Tuple3<String, String, String>>{

		@Override
		public void coGroup(
				Iterable<Tuple3<String, String, String>> edges,
				Iterable<Tuple2<String, String>> neighbors,
				Collector<Tuple3<String, String, String>> out)
				throws Exception {
			// TODO Auto-generated method stub
			
			Set<String> new_neighbor_set = new HashSet<String>(); //this way there is no double iteration
			
			for (Tuple2<String, String> val : neighbors) {
					new_neighbor_set.add(val.f1);
				}
			
			for(Tuple3<String, String, String> edge:edges){
				String vertex1 = edge.f0;
				String vertex2 = edge.f1;
				String old_neighbors = edge.f2;
				
				for(String new_neighbors:new_neighbor_set){
					String all_neighbors = old_neighbors+ " "+new_neighbors;
					
					out.collect(new Tuple3<String, String, String>(vertex1, vertex2, all_neighbors));
				}
			}
		}	
	}

	public static final class BackToString implements FlatMapFunction<Tuple2<String, Integer>,String>{

		@Override
		public void flatMap(Tuple2<String, Integer> in, Collector<String> out) throws Exception {
			out.collect(in.f0.concat(" ").concat(in.f1.toString()));
		}	
	}
	public static final class Unique implements ReduceFunction<Tuple2<String, Integer>>{

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> arg0,
				Tuple2<String, Integer> arg1) throws Exception {
			
			return new Tuple2<String,Integer>(arg0.f0,1);
		}
	}
	public static final class GenerateNeighborList implements ReduceFunction<Tuple2<String, String>>{

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> edge1,
				Tuple2<String, String> edge2) throws Exception {
			
			return new Tuple2<String,String>(edge1.f0,edge1.f1.concat(" ").concat(edge2.f1));
		}
	}
	//
	public static final class EdgeFilter implements FilterFunction<Tuple2<String,Integer>> {
		  @Override
		  public boolean filter(Tuple2<String,Integer> edge) {
			  return (Integer)edge.getField(1)>1;
		  }
		}
	public static final class NodeFilter implements FilterFunction<Tuple2<String,String>> {
		
		double verge=0;
		
		public NodeFilter(double i){
			verge=i;
		}
		@Override
		  public boolean filter(Tuple2<String,String> nodes) {
			  return Math.random()>verge;
		  }
		}
	public static final class WeightFilter implements FilterFunction<Tuple2<String,Integer>> {
		  @Override
		  public boolean filter(Tuple2<String,Integer> weighted_edge) {
			  return (Integer)weighted_edge.getField(1)>0;
		  }
		}
	public static final class MirroredEdges implements FlatMapFunction<String,Tuple2<String,String>>{

		@Override
		public void flatMap(String in,
				Collector<Tuple2<String, String>> out) throws Exception {
			// TODO Auto-generated method stub
			for(String edge:in.split("\\n")){
				String[] vertices = edge.split("\\s+"); //split by any number of spaces
				out.collect(new Tuple2<String,String>(vertices[0],vertices[1]));
				out.collect(new Tuple2<String,String>(vertices[1],vertices[0]));
			}
		}
	
	}	
	public static final class UniqueEdges implements FlatMapFunction<String,Tuple2<String,String>>{

		@Override
		public void flatMap(String in,
				Collector<Tuple2<String, String>> out) throws Exception {
			// TODO Auto-generated method stub
			for(String edge:in.split("\\n")){
				String[] vertices = edge.split("\\s+"); //split by any number of spaces
				out.collect(new Tuple2<String,String>(vertices[0],vertices[1]));
			}
		}
	}	
	
	
	public static final class Sorter implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.f0.toLowerCase().split("\\W+");

			// emit the pairs
			for (int i=0;i<tokens.length;i=i+2) {
				if (tokens[i+1].compareTo(tokens[i])<0) {
					String x = tokens[i+1];
					tokens[i+1]=tokens[i];
					tokens[i]=x;
				}	
				out.collect(new Tuple2<String, Integer>(tokens[i]+" "+tokens[i+1], 1));
			}
		}
	}
	
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
		
			for (String token:value.split("\\n")) {
					out.collect(new Tuple2<String, Integer>(token,1));
				}	
		}
	}
	public static final class NumberTurner implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>> {
		@Override
		public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<Integer, Integer>> out) {
		
					out.collect(new Tuple2<Integer, Integer>(value.f1,1));
					
		}
	}
}
