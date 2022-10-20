//package part1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

public class PDNodeWritable implements Writable {
	public static int MAX_INT = 2147000000;

    private int nodeID;
    private int distance;
    private int parentNodeID;
	private ArrayList<Map.Entry<Integer, Integer>> neighborList;

    public PDNodeWritable() { 
		this.nodeID = -2;
		this.distance = MAX_INT;
		this.parentNodeID = -1;
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
    }
	public PDNodeWritable(int nodeID){ //delete if not needed
		setNodeID(nodeID);
		this.distance = MAX_INT;
		this.parentNodeID = -1;
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
	}
	public PDNodeWritable(int nodeID, int distance, int parentNodeID){ //delete if not needed
		setNodeID(nodeID);
		setDistance(distance);
		setParentNodeID(parentNodeID);
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
	}
    public PDNodeWritable(PDNodeWritable node){ //make deep copy
		this.nodeID = node.getNodeID();
		this.distance = node.getDistance();
		this.parentNodeID = node.getParentNodeID();
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>(node.getNeighborList());
	}
	public int getNodeID(){
		return nodeID;
	}
	public void setNodeID(int nodeID){
		this.nodeID = nodeID;
	}

	public int getDistance(){
		return distance;
	}
	public void setDistance(int distance){
		this.distance = distance;
	}

	public int getParentNodeID(){
		return parentNodeID;
	}
	public void setParentNodeID(int parentNodeID){
		this.parentNodeID = parentNodeID;
	}
	public ArrayList<Map.Entry<Integer, Integer>> getNeighborList(){
		return neighborList;
	}
	public void setNeighborList(ArrayList<Map.Entry<Integer, Integer>> neighborList){
		this.neighborList = neighborList;
	}
	public void addNeighbor(int nodeID, int weight){
		Map.Entry<Integer, Integer> tmp = new AbstractMap.SimpleEntry<>(nodeID, weight);
		this.neighborList.add(tmp);
	}
	public void addNeighbor(int nodeID){
		Map.Entry<Integer, Integer> tmp = new AbstractMap.SimpleEntry<>(nodeID, 1);
		this.neighborList.add(tmp);
	}
	public String toString(){
		String output = Integer.toString(nodeID) + " " + Integer.toString(distance)
			+ " " + Integer.toString(parentNodeID) + " " + Integer.toString(neighborList.size());
		for(int i = 0; i < neighborList.size(); ++i){
			Map.Entry<Integer, Integer> tmp = neighborList.get(i);
			output += " " + Integer.toString(tmp.getKey()) + " " + Integer.toString(tmp.getValue());
		}
		return output;
	}
	//add additional functions if needed
	public void write(DataOutput out) throws IOException {
    	out.writeInt(nodeID);
		out.writeInt(distance);
		out.writeInt(parentNodeID);
		out.writeInt(neighborList.size());
		for(int i = 0; i < neighborList.size(); ++i){
			Map.Entry<Integer, Integer> tmp = neighborList.get(i);
			out.writeInt(tmp.getKey());
			out.writeInt(tmp.getValue());
		}
    }

    public void readFields(DataInput in) throws IOException {
		nodeID = in.readInt();
		distance = in.readInt();
		parentNodeID = in.readInt();
		int size = in.readInt();
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
		for(int i = 0; i < size; i++){
			int neighborNodeID = in.readInt();
			int weight = in.readInt();
			Map.Entry<Integer, Integer> tmp = new AbstractMap.SimpleEntry<>(neighborNodeID, weight);
			neighborList.add(tmp);
		}
    }
}