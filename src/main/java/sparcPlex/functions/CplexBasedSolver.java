package  sparcPlex.functions;

import sparcPlex.intermediateDataTypes.SolverResult;
import cplexLib.dataTypes.ActiveSubtree;
import cplexLib.dataTypes.NodeAttachment;
import cplexLib.dataTypes.Solution;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static cplexLib.constantsAndParams.Constants.*;
import static cplexLib.constantsAndParams.Parameters.PARTITION_ID;
import cplexLib.dataTypes.NodeAttachmentMetadata;
import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;
 
import scala.Tuple2;  
import static sparcPlex.constsAndParams.Parameters.*;


/**
 * 
 * @author SRINI
 *
 * Solves all subtrees in the partition one by one.
 * 
 * Decides the time slice for each subtree, depending on time allocated for the whole partition, clock time remaining, and the size of each sub tree.
 * Note that the users can supply (as parameter) the time slices recommended for solving easy/hard tree nodes .
 * In case some subtrees cannot be solved at all in an iteration, they are left alone ( to be picked up next iteration)
 * 
 * After solving each subtree, updates local best known optimum as needed.
 * Returns a SolverResult which contains
 *    a) list of nodes pending solution   ( which could be empty) , and 
 *    b) the best local solution found on this partition, if better than the bestKnownGlobalSolution.
 * 
 */
public class CplexBasedSolver  implements PairFlatMapFunction<Iterator<Tuple2<Integer,ActiveSubtree>>, Integer, SolverResult> , Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final Solution bestKnownGlobalSolution;
    private Solution bestKnownLocalOptimum;
    
    private final Instant endTimeOnWorkerMachine;
    
    private int easyNodesRemainingInPartition  =ZERO;
    private int hardNodesRemainingInPartition   =ZERO;
    
    private Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap ;
    //extract the list from th emap, for this partition
    private List<NodeAttachmentMetadata>  pendingNodesList;
    
    /**
     
     * 
     * pendingNodesList is the list of pending nodes on this partition. 
     * Each entry will contain the subtree ID to which this node belongs.
     * 
     **/
    public CplexBasedSolver (Instant endTimeOnWorkerMachine,        Solution bestKnownGlobalSolution, Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap  ){
        
        this.endTimeOnWorkerMachine=endTimeOnWorkerMachine;
        
        this.bestKnownGlobalSolution = bestKnownGlobalSolution ;  
        bestKnownLocalOptimum =bestKnownGlobalSolution;
        
        this.perPartitionPendingNodesMap=perPartitionPendingNodesMap;
       
    }

    public Iterable<Tuple2<Integer, SolverResult>> call(
            Iterator<Tuple2<Integer, ActiveSubtree>> iterator) throws Exception {
        
        //our return value
        List<Tuple2<Integer, SolverResult>> resultList = new ArrayList<Tuple2<Integer, SolverResult>>();
        SolverResult solverResult = new SolverResult();
                 
        Tuple2<Integer, ActiveSubtree> inputTuple = null;
        int partitionId = ZERO;
                        
        //process one subtree at a time 
        while ( iterator.hasNext()){
            
            inputTuple = iterator.next();
            partitionId = inputTuple._1;
            ActiveSubtree subTree = inputTuple._2;
            

            //note that we supply every subtree with its partition ID, for logging purposes. 
            //Every partition must have its own log, to prevent log corruption.
            //
            //set the PARTITION_ID  
            //No harm in setting the same value multiple times
            PARTITION_ID = partitionId;
            
             
            //find the number of easy and hard nodes in the partition
            //this will only be done the first time this while loop is entered
            if (easyNodesRemainingInPartition+hardNodesRemainingInPartition==ZERO) {
                pendingNodesList= perPartitionPendingNodesMap.get(partitionId);
                //initialize these counts
                for (NodeAttachmentMetadata nodeMetadata :pendingNodesList){
                    if (nodeMetadata.isEasy){
                        easyNodesRemainingInPartition ++;
                    } else{
                        hardNodesRemainingInPartition ++;
                    }                    
                }
            }
            
            //get time slice for this subtree
            long numberOfEasyNodesInSubTree =subTree.getNumLeafNodes(true);
            long numberOfHardNodesInSubtree =subTree.getNumLeafNodes(false);
            double timeSliceForSubTree = getTimeSliceForThisSubtree(  subTree, numberOfEasyNodesInSubTree,numberOfHardNodesInSubtree );
            
            //solve the subtree if we have been alloted at least a few seconds
            
            if (timeSliceForSubTree > ZERO  ){
                subTree.solve(timeSliceForSubTree,  bestKnownLocalOptimum.getObjectiveValue() );    
                
                //the subtree now has a solution and updated list of child nodes which 
                //we must collect, unless the subtree has
                //been discarded. If subtree solved to completion then it will have no pending kids.
                
                Solution subTreeSolution = subTree.getSolution() ;
                if ( ZERO != (new SolutionComparator()).compare(bestKnownLocalOptimum, subTreeSolution))
                    //we have found a better solution
                    bestKnownLocalOptimum = subTreeSolution;
                
                //update the result of solving this partition with the results form the tree just solved
                if (  !subTree.isDiscardable())                
                    solverResult.merge(bestKnownLocalOptimum, subTree.getMetadataForLeafNodesPendingSolution().values());
                                
            } //end if time slice > 0
            
            //update count of nodes still left for processing
            easyNodesRemainingInPartition-=numberOfEasyNodesInSubTree;
            hardNodesRemainingInPartition-=numberOfHardNodesInSubtree;
            
        }//end while - iterate over subtrees in partition
        
        //return results
        Tuple2<Integer, SolverResult> resultTuple = new Tuple2<Integer, SolverResult>(partitionId,solverResult );
        resultList.add(resultTuple );
        return resultList;
         
    }//end method call
   
    //time slice for this subtree is a fraction of the remaining time left for this partition
    //the fraction is calculated using the number of leaf nodes in this subtree, divided by the number of leaf nodes left in the partition
    private double getTimeSliceForThisSubtree(ActiveSubtree subTree, long numberOfEasyNodesInSubTree, long numberOfHardNodesInSubtree){
        
        double timeSliceForSubTree = ZERO;
        
        double wallClockTimeLeft = Duration.between(Instant.now(), this.endTimeOnWorkerMachine).toMillis()/THOUSAND;
        
        if (wallClockTimeLeft >ZERO && (numberOfEasyNodesInSubTree+numberOfHardNodesInSubtree>ZERO) ) {
            
            //we have some time left to solve trees on this partition
            
            //check to see if we have more time than expected for the hard nodes
            double timeLeftForHardNodesInPartition = 
                    wallClockTimeLeft  - easyNodesRemainingInPartition * EASY_NODE_TIME_SLICE_SECONDS;
            
            double surplusTime = timeLeftForHardNodesInPartition    - hardNodesRemainingInPartition*HARD_NODE_TIME_SLICE_SECONDS;
            
            if(surplusTime>ZERO){
                //we try to use up surplus time right away
                timeSliceForSubTree = surplusTime + 
                        numberOfHardNodesInSubtree*HARD_NODE_TIME_SLICE_SECONDS + 
                         + numberOfEasyNodesInSubTree*EASY_NODE_TIME_SLICE_SECONDS;
            } else {
                //divide remaining time equally between remaining subtrees. Calculate the fair share for this subtree.
                timeSliceForSubTree = wallClockTimeLeft *  (numberOfHardNodesInSubtree*HARD_NODE_TIME_SLICE_SECONDS    +numberOfEasyNodesInSubTree *EASY_NODE_TIME_SLICE_SECONDS) ;
                timeSliceForSubTree = timeSliceForSubTree/ (hardNodesRemainingInPartition*HARD_NODE_TIME_SLICE_SECONDS +easyNodesRemainingInPartition * EASY_NODE_TIME_SLICE_SECONDS) ;
                
                //solve subtree for at least EASY_NODE_TIME_SLICE_SECONDS , even if this sends us slightly over the time limit
                if (timeSliceForSubTree <EASY_NODE_TIME_SLICE_SECONDS ) timeSliceForSubTree=EASY_NODE_TIME_SLICE_SECONDS;
            }
            
        }  
        
        return timeSliceForSubTree  ;
        
    }// end method getTimeSliceForThisSubtree
    
}
