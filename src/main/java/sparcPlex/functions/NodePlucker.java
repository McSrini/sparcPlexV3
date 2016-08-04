/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.functions;

import static cplexLib.constantsAndParams.Constants.*;
import static cplexLib.constantsAndParams.Constants.ZERO;
import cplexLib.dataTypes.ActiveSubtree;
import cplexLib.dataTypes.*;
import java.io.Serializable;
import java.util.*;
import java.util.List;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author srini
 */
public class NodePlucker implements  PairFlatMapFunction<Iterator<Tuple2<Integer,ActiveSubtree>>, Integer, List<NodeAttachment >> ,Serializable{
    
    private Map <Integer, List<String >> nodesToPluckOut;
            
    public NodePlucker ( Map <Integer, List<String >> nodesToPluckOut) {
        
        this.nodesToPluckOut = nodesToPluckOut;
        
    }
 
    public Iterable<Tuple2<Integer, List<NodeAttachment>>> call(Iterator<Tuple2<Integer, ActiveSubtree>> iterator) throws Exception {
        
        //our return value
        List<Tuple2<Integer, List<NodeAttachment >>> resultList = new ArrayList<Tuple2<Integer, List<NodeAttachment >>>();
                 
        Tuple2<Integer, ActiveSubtree> inputTuple = null;
        int partitionId = ZERO;
        
        //farmed nodes from this partition 
        List<NodeAttachment > farmedNodeList = new ArrayList<NodeAttachment > ();
        
        //process one subtree at a time 
        while ( iterator.hasNext()){
                        
            inputTuple = iterator.next();
            partitionId = inputTuple._1;
            ActiveSubtree subTree = inputTuple._2;
            
            //nodes to pluck from this tree
            List<String> nodesIDsToPluck =        findNodeIDsToPluck(  subTree.getGUID(),   partitionId);
            
            //remove these nodes from the tree and collect them
            for (String nodeID : nodesIDsToPluck ){
                farmedNodeList.add(subTree.removeUnsolvedLeafNode(nodeID));
            }
            
        }
        
        Tuple2<Integer, List<NodeAttachment >> resultTuple = new Tuple2<Integer, List<NodeAttachment >>(partitionId, farmedNodeList);
        resultList.add(resultTuple);
        return resultList;
        
    }
    
    private List<String> findNodeIDsToPluck(String treeGUID, int partitionID){
        List<String> nodeIDList = new ArrayList<String>();
        List<String> treesAndNodes = nodesToPluckOut.get(partitionID);
        if(treesAndNodes !=null || treesAndNodes.size()>ZERO){
            //there are nodes to pluck from this partition
            for (String treeAndNode :treesAndNodes){
                String [] stringArray =treeAndNode.split(DELIMITER);
                String treeGuid = stringArray[ZERO];
                String nodeID= stringArray[ONE];
                if (treeGUID.equalsIgnoreCase(treeGuid)) nodeIDList.add(nodeID);
            }
        }
        return nodeIDList;
    }
          
}
