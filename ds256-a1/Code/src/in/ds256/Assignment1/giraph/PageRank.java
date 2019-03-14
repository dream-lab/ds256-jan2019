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

/**Reference : https://github.com/usi-systems/giraph-pagerank/blob/master/pagerank/PageRank.java **/

public class PageRank extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	
	private static int NUM_SUPERSTEPS = 10;
	private static double DAMPENING_FACTOR = 0.85;
	private static double EPSILON = 0.0001;

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) {
		double pageRank_par = 0, pageRankOld = 0;
		boolean changed = false;
		
		if (getSuperstep() == 0)
			vertex.setValue(new DoubleWritable(1.0));
		else if (getSuperstep() >= 1) {
			
			pageRankOld = vertex.getValue().get();
			
			for (DoubleWritable message : messages) {
				pageRank_par += message.get();
			}
			
			vertex.setValue(new DoubleWritable(0.15 + DAMPENING_FACTOR * pageRank_par));
			changed = Math.abs(pageRankOld - vertex.getValue().get()) < EPSILON;
			
		}
		if (!changed && getSuperstep() < NUM_SUPERSTEPS) {
			
			int numAdjEdges = vertex.getNumEdges();
			DoubleWritable pageRankCurr = new DoubleWritable(vertex.getValue().get() / numAdjEdges);
			for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges())
				sendMessage(edge.getTargetVertexId(), pageRankCurr);
			
		}

		System.out.println("PageRank Superstep: " + getSuperstep());
		vertex.voteToHalt();
	}
}
