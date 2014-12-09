package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class UnDirector {
	public static void main(String[] args) throws Exception {

		// 1. set up the execution environment - typically done this way
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// 2. get input data - from file
		DataSet<String> text = env.readTextFile("file:///home/alex/Desktop/IMPRO/flinkxample/directedEdges");		
		
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
		
		
		// 4. specify where results go - in this case: write in file
		edges.writeAsText("file:///home/alex/Desktop/IMPRO/flinkxample/undirectedEdgesOutput");

		// 5. execute program
		env.execute("WordCount Example");
	}
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
		
			for (String token:value.split("\\n")) {
					out.collect(new Tuple2<String, Integer>(token,1));
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
	
	public static final class Unique implements ReduceFunction<Tuple2<String, Integer>>{
	
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> arg0,
				Tuple2<String, Integer> arg1) throws Exception {
			
			return new Tuple2<String,Integer>(arg0.f0,1);
		}
	}
	public static final class EdgeFilter implements FilterFunction<Tuple2<String,Integer>> {
		  @Override
		  public boolean filter(Tuple2<String,Integer> edge) {
			  return (Integer)edge.getField(1)>1;
		  }
		}
	public static final class BackToString implements FlatMapFunction<Tuple2<String, Integer>,String>{
	
		@Override
		public void flatMap(Tuple2<String, Integer> in, Collector<String> out) throws Exception {
			out.collect(in.f0);
		}	
	}
}
