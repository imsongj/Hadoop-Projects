//package part2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

public class PRNodeWritable implements Writable {
    private int nodeID;
    private double pagerank;
	private ArrayList<Integer> neighborList;

    public PRNodeWritable() { 
		this.nodeID = -2;
		this.pagerank = 0;
		this.neighborList = new ArrayList<Integer>();
    }
	public PRNodeWritable(int nodeID){ //delete if not needed
		setNodeID(nodeID);
		this.pagerank = 0;
		this.neighborList = new ArrayList<Integer>();
	}
	public PRNodeWritable(int nodeID, double pagerank){ //delete if not needed
		setNodeID(nodeID);
		setPagerank(pagerank);
		this.neighborList = new ArrayList<Integer>();
	}
    public PRNodeWritable(PRNodeWritable node){ //make deep copy
		this.nodeID = node.getNodeID();
		this.pagerank = node.getPagerank();
		this.neighborList = new ArrayList<Integer>(node.getNeighborList());
	}
	public int getNodeID(){
		return nodeID;
	}
	public void setNodeID(int nodeID){
		this.nodeID = nodeID;
	}

	public double getPagerank(){
		return pagerank;
	}
	public void setPagerank(double pagerank){
		this.pagerank = pagerank;
	}

	public ArrayList<Integer> getNeighborList(){
		return neighborList;
	}
	public void setNeighborList(ArrayList<Integer> neighborList){
		this.neighborList = neighborList;
	}
	public int getNeighborSize(){
		return neighborList.size();
	}
	public void addNeighbor(int nodeID){
		this.neighborList.add(nodeID);
	}

	public String toString(){
		String output = Integer.toString(nodeID) + " " + Double.toString(pagerank)
			+ " " + Integer.toString(neighborList.size());
		for(int i = 0; i < neighborList.size(); ++i){
			output += " " + Integer.toString(neighborList.get(i));
		}
		return output;
	}
	//add additional functions if needed
	public void write(DataOutput out) throws IOException {
    	out.writeInt(nodeID);
		out.writeDouble(pagerank);
		out.writeInt(neighborList.size());
		for(int i = 0; i < neighborList.size(); ++i){
			out.writeInt(neighborList.get(i));
		}
    }

    public void readFields(DataInput in) throws IOException {
		nodeID = in.readInt();
		pagerank = in.readDouble();
		int size = in.readInt();
		this.neighborList = new ArrayList<Integer>();
		for(int i = 0; i < size; i++){
			neighborList.add(in.readInt());
		}
    }
}