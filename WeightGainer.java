package org.apache.flink;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class WeightGainer {
	public static void main(String[] args) throws Exception {

		// 1. set up the execution environment - typically done this way
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// 2. get input data - from file
		DataSet<String> text = env.readTextFile("file:///home/alex/Desktop/IMPRO/flinkxample/undirectedEdges0");		
		

		DataSet<Tuple2<Long, Long>> uniqueedges =
				text
				.flatMap(new UniqueEdges());	//all edges unique, sorted alphabetically by vertex
		
		DataSet<Tuple2<Long, Long>> mirrorededges =
				text
				.flatMap(new MirroredEdges());	//all edges mirrored (vertices swapped) 
				
		DataSet<Tuple2<Long, Long[]>> neighborlist =	
				mirrorededges					//uses mirrored edges so every vertex is captured
				.groupBy(0)
				.reduceGroup(new NeighborCollector2()) //outputs (vertex;{all neighbors})
				//.filter(new NodeFilter(0.25)) //DEGREE DISTRIBUTION cancel some random neighbors by verge in brackets (for Math.random)
				;	
				
		DataSet<Tuple3<Long, Long, Long[]>> firstjoin =
				uniqueedges
				.coGroup(neighborlist)			//uses neighbor list
				.where(0).equalTo(0)			//joins on start-V
				.with(new Linker1())			//outputs (start-V,end-V,neighbors of start-V)
				;
		
		DataSet<Tuple3<Long, Long, Long[]>> secondjoin=
				firstjoin
				.coGroup(neighborlist).			//uses neighbor list
				where(1).equalTo(0).			//joins on end-V
				with(new Linker2())				//outputs (start-V,end-V,neighbors of start-V AND end-V)
				;
		DataSet<Tuple3<Long, Long, Integer>> finalgraph=
				secondjoin
				.flatMap(new FinalList())		//outputs (start-V,end-V,count of duplicate neighbors)
				.filter(new WeightFilter())		//only allows for edges with weight >0
				//.flatMap(new BackToString()) 	//converts back to string
				;
//		DEGREE DISTRIBUTION
//		DataSet<Tuple2<Integer,Integer>> degrees=
//				finalgraph
//				.flatMap(new NumberTurner())
//				.groupBy(0)
//				.aggregate(Aggregations.SUM, 1)
//				;
		
		// 4. specify where results go - in this case: write in file
		finalgraph.writeAsText("file:///home/alex/Desktop/IMPRO/flinkxample/weightedEdges"+Math.random());

		// 5. execute program
		env.execute("WordCount Example");
	}
	//User defined functions
	public static final class UniqueEdges implements FlatMapFunction<String,Tuple2<Long,Long>>{
		
		@Override
		public void flatMap(String in, Collector<Tuple2<Long,Long>> out) throws Exception {
			for(String edge:in.split("\\n")){ //split by lines
				String[] stringVertices = edge.split("\\s+"); //split by any number of spaces
				Long[] longVertices={Long.parseLong(stringVertices[0]),Long.parseLong(stringVertices[1])}; //parse String to Long
				//Long[] out1={longVertices[1]}; //following function requires array
				out.collect(new Tuple2<Long,Long>(longVertices[0],longVertices[1]));
			}
		}
	}
	public static final class MirroredEdges implements FlatMapFunction<String,Tuple2<Long,Long>>{
		
		@Override
		public void flatMap(String in, Collector<Tuple2<Long,Long>> out) throws Exception {
			for(String edge:in.split("\\n")){ //split by lines
				String[] stringVertices = edge.split("\\s+"); //split by any number of spaces
				Long[] longVertices={Long.parseLong(stringVertices[0]),Long.parseLong(stringVertices[1])}; //parse String to Long
				//Long[] out1={longVertices[1]}; //following function requires array
				//Long[] out0={longVertices[0]}; //following function requires array
				out.collect(new Tuple2<Long,Long>(longVertices[0],longVertices[1]));
				out.collect(new Tuple2<Long,Long>(longVertices[1],longVertices[0])); //Output vertices second time in reversed order
			}
		}
	}	
	
	
	public static final class FinalList implements FlatMapFunction<Tuple3<Long,Long,Long[]>,Tuple3<Long,Long,Integer>>{
			
			public Integer countDuplicateNeighbors(Tuple3<Long,Long,Long[]> s){
				int count=0;
		
				Long[] neighbors=s.f2;
				Arrays.sort(neighbors);
			
				for(int i=0; i<neighbors.length-1;i++){
					if(neighbors[i].equals(neighbors[i+1])){count++;}
				}
				return count;
			}
			
			@Override
			public void flatMap(Tuple3<Long,Long,Long[]> in,Collector<Tuple3<Long,Long,Integer>> out) throws Exception {
				
				out.collect(new Tuple3<Long,Long,Integer>(in.f0,in.f1,countDuplicateNeighbors(in)));
				
			}
		}



	public static final class Linker1 implements CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>, Tuple3<Long, Long, Long[]>> {
		@Override
		public void coGroup(Iterable<Tuple2<Long, Long>> edges, Iterable<Tuple2<Long, Long[]>> neighbors, Collector<Tuple3<Long, Long, Long[]>> out) throws Exception {
			Set<Long> new_neighbor_set=new HashSet<Long>();
			for (Tuple2<Long, Long[]> val : neighbors) {
				for(Long nb:val.f1){
					new_neighbor_set.add(nb);
				} 
			}
			Long[] new_neighbors=new_neighbor_set.toArray(new Long[new_neighbor_set.size()]);
			for (Tuple2<Long, Long> edge : edges) {
				Long vertex1 = edge.f0;
				Long vertex2 = edge.f1;
				out.collect(new Tuple3<Long, Long, Long[]>(vertex1,vertex2, new_neighbors));	
			}
		}
	}
	public static final class Linker22 implements JoinFunction<Tuple3<Long, Long, Long[]>,Tuple2<Long, Long[]>,Tuple3<Long, Long, Long[]>>{


		@Override
		public Tuple3<Long, Long, Long[]> join(Tuple3<Long, Long, Long[]> edges,Tuple2<Long, Long[]> neighbors) throws Exception {
			Long[] all_neighbors= (Long[]) ArrayUtils.addAll(edges.f2, neighbors.f1);
			Long vertex1 = edges.f0;
			Long vertex2 = edges.f1;
			return(new Tuple3<Long, Long, Long[]>(vertex1, vertex2, all_neighbors));
		}

	}
	public static final class Linker2 implements CoGroupFunction<Tuple3<Long, Long, Long[]>,Tuple2<Long, Long[]>,Tuple3<Long, Long, Long[]>> {
		@Override
		public void coGroup(Iterable<Tuple3<Long, Long, Long[]>> edges,
				Iterable<Tuple2<Long, Long[]>> neighbors,
				Collector<Tuple3<Long, Long, Long[]>> out) throws Exception  {
			// TODO Auto-generated method stub
			Set<Long> new_neighbor_set = new HashSet<Long>(); 
			for (Tuple2<Long, Long[]> val : neighbors) {
				for(Long val2:val.f1){
					new_neighbor_set.add(val2);
				}
			}
			Long[] new_neighbors=new_neighbor_set.toArray(new Long[new_neighbor_set.size()]);
			
			for (Tuple3<Long, Long, Long[]> edge : edges) {
				Long vertex1 = edge.f0;
				Long vertex2 = edge.f1;
				Long[] old_neighbors = edge.f2;
				Long[] all_neighbors=(Long[]) ArrayUtils.addAll(new_neighbors, old_neighbors);
	
					out.collect(new Tuple3<Long, Long, Long[]>(vertex1,vertex2, all_neighbors));
				
			}
		}
	}
	public static final class NeighborCollector implements ReduceFunction<Tuple2<Long, Long[]>>{
	
		@Override
		public Tuple2<Long, Long[]> reduce(Tuple2<Long, Long[]> neighbor1, Tuple2<Long, Long[]> neighbor2) throws Exception {
			Long node=neighbor1.f0; //node in question
			Long[] collectedNeighbors=neighbor1.f1; //gathered neighbors of node
			Long nextNeighbor= neighbor2.f1[0]; //next neighbor of node
			Long[] neighbors=new Long[collectedNeighbors.length+1];
			for(int i=0;i<collectedNeighbors.length;i++){neighbors[i]=collectedNeighbors[i];}
			neighbors[collectedNeighbors.length]=nextNeighbor;
			return new Tuple2<Long, Long[]>(node,neighbors);
		}
	}
	
	public static final class NeighborCollector2 implements GroupReduceFunction<Tuple2<Long,Long>,Tuple2<Long,Long[]>>{

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> in, Collector<Tuple2<Long, Long[]>> out) throws Exception {
			Set<Long> neighbors=new HashSet<Long>();
			Long key=null;
			for(Tuple2<Long,Long> t:in){
				neighbors.add(t.f1);
				key=t.f0;
			}
			Long[] neighbors2 = neighbors.toArray(new Long[neighbors.size()]);
			out.collect(new Tuple2<Long,Long[]>(key,neighbors2));
		}
		
	}
	//
	
	public static final class NodeFilter implements FilterFunction<Tuple2<Long,Long[]>> {		
		double verge=0;	
		public NodeFilter(double i){
			verge=i;
		}
		@Override
		  public boolean filter(Tuple2<Long,Long[]> nodes) {
			  return Math.random()>verge;
		  }
	}

	public static final class WeightFilter implements FilterFunction<Tuple3<Long,Long,Integer>> {
		 
		@Override
		public boolean filter(Tuple3<Long, Long, Integer> weighted_edge)throws Exception {
		
			return (Integer)weighted_edge.getField(2)>0;
	
		}
	}
		

}
