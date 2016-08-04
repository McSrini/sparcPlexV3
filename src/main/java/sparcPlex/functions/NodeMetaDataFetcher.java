/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.functions;

import static cplexLib.constantsAndParams.Constants.ZERO;
import cplexLib.dataTypes.ActiveSubtree;
import cplexLib.dataTypes.*;
import cplexLib.dataTypes.NodeAttachmentMetadata;
import java.io.Serializable;
import java.util.Iterator;
import java.util.*;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import sparcPlex.intermediateDataTypes.SolverResult;

/**
 *
 * @author srini
 * 
 * from each partition, fetch how many unsolved child nodes are there
 */
public class NodeMetaDataFetcher implements  PairFlatMapFunction<Iterator<Tuple2<Integer,ActiveSubtree>>, Integer, List<NodeAttachmentMetadata>> ,Serializable{
 
    public Iterable<Tuple2<Integer, List<NodeAttachmentMetadata>>> call(Iterator<Tuple2<Integer, ActiveSubtree>> iterator) throws Exception {
        
        //our return value
        List<Tuple2<Integer, List<NodeAttachmentMetadata>>> resultList = new ArrayList<Tuple2<Integer, List<NodeAttachmentMetadata>>>();
                 
        Tuple2<Integer, ActiveSubtree> inputTuple = null;
        int partitionId = ZERO;
        
        //list of all node attachments on this partition
        List<NodeAttachmentMetadata> attachmentList = new ArrayList<NodeAttachmentMetadata>();
        
        //process one subtree at a time 
        while ( iterator.hasNext()){
            
            inputTuple = iterator.next();
            partitionId = inputTuple._1;
            ActiveSubtree subTree = inputTuple._2;
            
            for (NodeAttachmentMetadata  element :  subTree.getMetadataForLeafNodesPendingSolution().values()) {
                 attachmentList.add (element );
            }
            
        }
        
        Tuple2<Integer, List<NodeAttachmentMetadata>> resultTuple = new Tuple2<Integer, List<NodeAttachmentMetadata>>(partitionId, attachmentList);
        resultList.add(resultTuple);
        return resultList;
    }
    
}
