/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.loadbalance;

import static cplexLib.constantsAndParams.Constants.*;
import cplexLib.dataTypes.*;
import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.*;
import org.apache.log4j.Logger;
import static sparcPlex.constsAndParams.Constants.*;
import static sparcPlex.constsAndParams.Parameters.*;
import sparcPlex.main.Driver;

/**
 *
 * @author srini
 * 
 * calculates the average number of leafs on every partition, and ensures that 
 * every partition is at least at (average-1) leafs after movement
 * 
 */
public class AveragingHueristic {
    
    private long thresholdLeafCount = ZERO;
    private Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap;
    
    //make note of ho wmany leafs each partition has, at the outset
    private Map <Integer, Integer > currentLeafCountMap = new HashMap<Integer, Integer >();
    
    //from each partiton, ho wmany to move?
    //-ve number indicates movement away
    private Map <Integer, Integer > howManyToMove = new HashMap<Integer, Integer >();
    
    private Random random = new Random(ONE);
    
    //form partition ID key, pluck out list of nodes
    //each element in the list is treeGUID concatenated with nodeID
    public Map <Integer, List<String >> nodesToPluckOut = new HashMap <Integer,  List<String > >();
    
    //this map dictates where the plucked out nodes are going
    public Map <Integer, Integer> nodesToMoveIn = new HashMap <Integer, Integer>();
    
    private static Logger logger=Logger.getLogger(AveragingHueristic.class);
    private static boolean isLoggingInitialized =false ;
    static {
        logger.setLevel(Level.DEBUG);
        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");         
        try {
            logger.addAppender(new RollingFileAppender(layout, DRIVER_LOG_FILE));
            isLoggingInitialized= true;
        } catch (IOException ex) {
            //logging not available, do not use logging in this file
        }
    }
    
    public AveragingHueristic (Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap ) {
        this.perPartitionPendingNodesMap =  perPartitionPendingNodesMap;
        findThreshold ();
       
        thresholdLeafCount = Math.min(MIN_LEAFS_PER_PARTITION,thresholdLeafCount);
       
        for (Integer partitionID : perPartitionPendingNodesMap.keySet()){
           //initialize
           howManyToMove.put(partitionID, ZERO);
        }
       
        while (ZERO < this.moveOne()) {  }
       
        //now we know how many to move from where to where
       
        //let us find which nodes to pluck out from each source partition
        findNodesToPluck ( );
               
        //now mark the partitions that are the recievers of plucked out nodes, and how many each will recieve
        findNodesToMoveIn();
                 
        if (  isLoggingInitialized) logger.info( "AveragingHueristic completed");
       
    }
    
    //this function populates nodesToMoveIn, basically deciding how many nodes to recieve into which partition
    private void findNodesToMoveIn() {
        for (Entry <Integer, Integer > entry :  howManyToMove.entrySet()){
            int partitionId = entry.getKey();
            int count = entry.getValue();
            if (count > ZERO) {
                
                //this partition is a reciever
                nodesToMoveIn.put(partitionId, count);
            }
        }
    }
    
    //this method pouplates nodesToPluckOut
    private void findNodesToPluck ( ) {
        
        for (Entry <Integer, Integer > entry :  howManyToMove.entrySet()){
            int partitionId = entry.getKey();
            int count = entry.getValue();
            if (count < ZERO) {
                
                //pluck |count| nodes
                List<String> nodesToPluck = findNodesToPluck (   partitionId,  - count);
                nodesToPluckOut.put(partitionId, nodesToPluck) ;
            }
        }
        
    }
    
    //randomly pluck one node at a time , until count nodes plucked
    private List<String> findNodesToPluck ( int partitionId, int count) {
        List<String> nodesToPluck = new ArrayList<String> () ;
        
        List<NodeAttachmentMetadata> nodeNetadataList = perPartitionPendingNodesMap.get(partitionId);
        
        while (count >ZERO){
            NodeAttachmentMetadata meta = nodeNetadataList.remove(random.nextInt( nodeNetadataList.size()));
            nodesToPluck.add(meta.treeGuid+DELIMITER + meta.nodeID);
            count = count -ONE;
        }        
        
        return nodesToPluck;
    }
    
    
    
    private int moveOne() {
        int countMoved = ZERO;
        
        //as long as someone is below the threshold, get them one node from someone highest above the threshold
        int poorestPartitionID = findPoorestPartition();
        int richestPartitionID = this.findRichestPartition();
        if (currentLeafCountMap.get(poorestPartitionID) < this.thresholdLeafCount &&
            currentLeafCountMap.get(richestPartitionID) > this.thresholdLeafCount    ) {
            
                //move 1 node
                currentLeafCountMap.put(poorestPartitionID,currentLeafCountMap.get(poorestPartitionID) +ONE);
                currentLeafCountMap.put(richestPartitionID,currentLeafCountMap.get(richestPartitionID) -ONE);
                howManyToMove.put( poorestPartitionID, howManyToMove .get(poorestPartitionID) +ONE);
                howManyToMove.put( richestPartitionID,howManyToMove.get(richestPartitionID) -ONE  );
                countMoved = ONE;
        }
        
        return countMoved;
    }
    
    
    private int findPoorestPartition(){
        int partitionID = -ONE;
        long highCount = PLUS_INFINITY;
        for (Entry <Integer, Integer > entry : currentLeafCountMap.entrySet()){
            if (entry.getValue()<highCount){
                partitionID = entry.getKey();
                highCount = entry.getValue();
            }
        }
        return partitionID;
    }
    
    private int findRichestPartition(){
        int partitionID = -ONE;
        long highCount = -ONE;
        for (Entry <Integer, Integer > entry : currentLeafCountMap.entrySet()){
            if (entry.getValue()>highCount){
                partitionID = entry.getKey();
                highCount = entry.getValue();
            }
        }
        return partitionID;
    }
    
    private void  findThreshold () {
        double count = ZERO ;
        for (Entry <Integer, List<NodeAttachmentMetadata>> entry : perPartitionPendingNodesMap.entrySet()){
            count +=entry.getValue().size();
            currentLeafCountMap.put( entry.getKey(), entry.getValue().size() );
        }
        thresholdLeafCount = -ONE + (long) Math.ceil(count/perPartitionPendingNodesMap.size());
    }
}
