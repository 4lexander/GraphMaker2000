package de.tu_berlin.impro3.flink.etl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class UnDirector {
	public static void main(String[] args) throws Exception {
		String in="file:///home/alex/workspace/quickstart/edges";
		String out="file:///home/alex/workspace/quickstart/undirectedEdges";
		run(in,out);
	}
	public static void run(String in, String out) throws Exception{
		// 1. set up the execution environment - typically done this way
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// 2. get input data - from file
		DataSet<String> text = env.readTextFile(in);		
		
		DataSet<String> edges =
				text
				.flatMap(new LineSplitter())	//splits String in lines
				.groupBy(0,1)
				.reduce(new Unique())			//cancels out all duplicate edges, should there be any
				.flatMap(new Sorter())			//sort edges alphabetically by vertex
				.groupBy(0,1)
				.aggregate(Aggregations.SUM, 2)	//count number of alphabetically sorted edges; can be two (bidirectional) or one (unidirectional)
				.filter(new EdgeFilter())		//allows only for edges with a count of >1 (i.e. 2), because they are mutual
				.flatMap(new BackToString()); 	//converts back to string; only "real", undirected edges from here

		// 4. specify where results go - in this case: write in file
		edges.writeAsText(out);

		// 5. execute program
		env.execute("Make Directed Mutual Edges Undirected");
	}
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<Long, Long>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<Long, Long>> out) {
		
			for (String edge:value.split("\\n")) {
				String[] vertices=edge.split("\\s+");
				out.collect(new Tuple2<Long, Long>(Long.parseLong(vertices[0]),Long.parseLong(vertices[1])));
				
			}	
		}
}
	public static final class Sorter implements FlatMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Integer>> {

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple3<Long, Long, Integer>> out) {
			// normalize and split the line
			Long vertex1=edge.f0;
			Long vertex2=edge.f1;
			// emit the pairs
				if (vertex2<vertex1) {out.collect(new Tuple3<Long, Long, Integer>(vertex2,vertex1,1));}	
				else {out.collect(new Tuple3<Long, Long, Integer>(vertex1,vertex2,1));}
		
		}
	}
	
	public static final class Unique implements ReduceFunction<Tuple2<Long, Long>>{

		@Override
		public Tuple2<Long, Long> reduce(
				Tuple2<Long, Long> arg0,
				Tuple2<Long, Long> arg1) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<Long, Long>(arg0.f0,arg0.f1);
		}
	}
	public static final class EdgeFilter implements FilterFunction<Tuple3<Long, Long, Integer>> {
		  @Override
		  public boolean filter(Tuple3<Long, Long, Integer> edge) {
			  return (Integer)edge.getField(2)>1;
		  }
		}
	public static final class BackToString implements FlatMapFunction<Tuple3<Long,Long, Integer>,String>{
		@Override
		public void flatMap(Tuple3<Long, Long, Integer> in,Collector<String> out) throws Exception {
			StringBuffer sb = new StringBuffer();
			sb.append(in.f0);
			sb.append(" ");
			sb.append(in.f1);
			out.collect(sb.toString());
			
		}	
	}
}
