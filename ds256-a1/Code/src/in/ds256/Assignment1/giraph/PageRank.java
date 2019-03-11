package in.ds256.Assignment1.giraph;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.zookeeper.server.quorum.Vote;

import java.io.IOException;
import java.lang.Math;

public class PageRank extends  BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	
	private static final Logger LOG = Logger.getLogger(PageRank.class);
	private static int NUM_SUPERSTEPS = 10;
	private static double DAMPENING_FACTOR = 0.85;
	private static double EPSILON = 0.1;
	private static double pageRankOld = 0.0;
	private static double pageRankCurr = 0.0;
	private static boolean changed = true;
	
	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages)
			throws IOException {	
		
		if(getSuperstep()==0) { /** The default page rank we set to each vertex is 1.0 **/
			vertex.setValue(new DoubleWritable(1.0));
			
		}else if(getSuperstep() >= 1 && getSuperstep()<= NUM_SUPERSTEPS) {
			
			pageRankOld = vertex.getValue().get();
			pageRankCurr = 0.0;
			
			for(DoubleWritable message: messages) {
				pageRankCurr = pageRankCurr + message.get();
			}
			
			pageRankCurr = 0.15 + 0.85*pageRankCurr;
			
			Double diff = pageRankOld - pageRankCurr ;
			
			if(diff <= EPSILON)
				changed = false;
			else
				changed = true;
		}
		
		if(getSuperstep()<= NUM_SUPERSTEPS && changed == true ) {
			int numVertices = vertex.getNumEdges();
			/** This is important **/
			/** This tells how much contribution of pagerank is going to each vertex **/
			DoubleWritable pageRankToSend = new DoubleWritable(vertex.getValue().get()/numVertices); 
						
			for(Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				sendMessage(edge.getTargetVertexId(), pageRankToSend);
			}
		}
		
		vertex.voteToHalt();
		
	}
	
}