/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.main;

import sparcPlex.functions.FilterOutDiscardedAndCompletedSubtrees;
import sparcPlex.functions.AttachmentConverter;
import cplexLib.dataTypes.ActiveSubtree;
import cplexLib.dataTypes.NodeAttachment;
import cplexLib.dataTypes.NodeAttachmentMetadata;
import cplexLib.dataTypes.Solution;
import org.apache.log4j.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;

import static cplexLib.constantsAndParams.Constants.*;
import static cplexLib.constantsAndParams.Parameters.*;
import static sparcPlex.constsAndParams.Constants.*;
import static sparcPlex.constsAndParams.Parameters.*;
import sparcPlex.functions.PartitionKeyAdder;
import java.time.*;
import java.util.*; 
import java.util.Map.Entry;
import scala.Tuple2;
import sparcPlex.functions.NodeMetaDataFetcher;
import sparcPlex.functions.SolutionComparator;
import sparcPlex.functions.*;
import sparcPlex.intermediateDataTypes.SolverResult;
import sparcPlex.loadbalance.AveragingHueristic;

/**
 *
 * @author srini
 * 
 * Driver for the SpracPlex framework
 * Use the CPlex library to distribute a BnB computation over Spark
 * 
 * 
 */
public class Driver {
    
    private static Logger logger=Logger.getLogger(Driver.class);
    
    
    public static void main(String[] args) throws Exception   {

        logger.setLevel(Level.DEBUG);
        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");         
        logger.addAppender(new RollingFileAppender(layout, DRIVER_LOG_FILE));

        //Driver for distributing the CPLEX  BnB solver on Spark
        SparkConf conf = new SparkConf().setAppName("SparcPlex V3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //We have an RDD which holds the frontier, i.e. subtrees with unsolved tree nodes
        //the key is the partition number
        JavaPairRDD < Integer, ActiveSubtree> frontier ; 
                
        //Initially the frontier only has the original problem  
        // Root node of the BnB tree is represented by an empty node attachment, i.e. no branching variables  
        //
        //let us start this root node on partition 1, partition 0 is the driver
        //
        // Note how the RDD is created, we never shuffle ActiveSubTree objects ; Only node attachments are shuffled across the network
        
        List<NodeAttachment> initialNodeList  = new ArrayList<NodeAttachment> ();
        initialNodeList.add(new NodeAttachment());
        frontier =sc.parallelize(initialNodeList) 
                /* add the key # of the partition we want to place it on*/
                .mapToPair( new PartitionKeyAdder(ONE)) 
                /*realize the movement to desired partition*/
                .partitionBy(new HashPartitioner(NUM_AVAILABLE_CORES)) 
                /* finally convert the attachment into an ActiveSubTree object*/
                .mapValues( new AttachmentConverter() ); 
        
        
        //For each partition , we maintain a list of pending nodes in each subtree on that partition
        //Recall that each NodeAttachmentMetadata has the subtree ID, and details about the node
        Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap = new HashMap <Integer,List<NodeAttachmentMetadata> > ();
        //go out to the cluster and find now many unsolved kids each partition has
        JavaPairRDD < Integer, List<NodeAttachmentMetadata>> perPartitionPendingNodesRDD = frontier.mapPartitionsToPair(new NodeMetaDataFetcher(), true);
        //no need to cache the RDD as it is used only once
        perPartitionPendingNodesMap= perPartitionPendingNodesRDD.collectAsMap();
        //At this point, we should have empty lists in the perPartitionPendingNodesMap for all partitions excep partition 1, which should have the original problem
                       
        //initialize  the incumbent 
        Solution incumbent = new Solution( ); 
        boolean isSolutionHalted=   false;
        
        //loop till frontier is empty, or some iteration count or time limit is exceeded
        int iteration = ZERO;
        for (;iteration <MAX_ITERATIONS;iteration++){
                        
            //STEP 0 : 
            //*****************************************************************************************************************
            //Prepare for this iteration, if it is needed at all.
            //Frontier is used many times, so cache it.
            frontier.cache();
            long treecount = frontier.count();
            if ( treecount == ZERO) {                
                //we are done, no active subtrees left to solve
                break;                 
            } else {
                logger.debug("Starting iteration " + iteration+", with "+treecount +" trees.") ;
            }
            
            //Use the perPartitionPendingNodesMap to decide cycle iteration time
            //Every partition must stop on or before this clock time
            Instant endTimeOnWorkerMachines = getIterationEndTime( perPartitionPendingNodesMap);
            
            //STEP 1 : 
            //*****************************************************************************************************************
            //Solve for some time. From each partition, we get the the best solution and pending kids.
            //Note that child nodes are represented by their metadata, which contains node ID and the containing subtree's unique ID
            CplexBasedSolver cplexSolver = new CplexBasedSolver(  endTimeOnWorkerMachines,  incumbent ,  perPartitionPendingNodesMap);
            JavaPairRDD< Integer, SolverResult> resultsRDD  = frontier.mapPartitionsToPair( cplexSolver, true);
            //RDD is used more than once, so cache it
            resultsRDD.cache(); 
            
            
            //STEP 2 : 
            //*****************************************************************************************************************
            //use the solutions found in this iteration to update the incumbent
            Map< Integer, SolverResult> resultsMap= resultsRDD.collectAsMap();
            Collection<SolverResult> solverResultCollection = resultsMap.values();
            
            for (SolverResult solverResult:solverResultCollection){     
                
                Solution solutionFromPartition =  solverResult.getSolution();
                
                if (  solutionFromPartition.isUnbounded()) {
                     logger.info("Solution is unbounded, will exit.");
                     isSolutionHalted= true;
                     break;
                }
                if (  solutionFromPartition.isError()) {
                     logger.error("Solution is in error, will exit.");
                     isSolutionHalted= true;
                     break;
                }                
                if ( ZERO != (new SolutionComparator()).compare(incumbent, solutionFromPartition))
                    //we have found a better solution
                    incumbent = solutionFromPartition;                
            }
            
            if (   isSolutionHalted) break;
            
            
            //STEP 3 : 
            //*****************************************************************************************************************
            //clean up all the discarded and completed subtrees from the frontier RDD
            JavaPairRDD < Integer, ActiveSubtree> filteredFrontier = frontier.filter(new FilterOutDiscardedAndCompletedSubtrees()) ; 
            //RDD is used many times
            filteredFrontier.cache() ;
            long filteredFrontierCount      =      filteredFrontier.count( );
            //if no trees left , we are done
            if ( filteredFrontierCount  == ZERO) {                
                //we are done, no active subtrees left to solve
                logger.info("All subtrees solved.");
                break;                 
            } 
            
            
            //STEP 4 : 
            //*****************************************************************************************************************
            // update perPartitionPendingNodesMap  
            
            for (Entry<Integer, SolverResult> entry   :resultsMap.entrySet()){
                perPartitionPendingNodesMap.put ( entry.getKey(), entry.getValue().getNodeList() );
            }
            
            //if perPartitionPendingNodesMap is empty , no farming needs to be done
            if (perPartitionPendingNodesMap.isEmpty() ){
                //skip step 5 , 6, and 7
                
                //STEP 8 : 
                //*****************************************************************************************************************
                //merge migrated nodes with existing frontier
                frontier = filteredFrontier;
                
            } else {

                //STEP 5 : 
                //*****************************************************************************************************************
                //use perPartitionPendingNodesMap to decide how to move nodes
                AveragingHueristic loabBalancingHueristic = new AveragingHueristic(perPartitionPendingNodesMap);
                Map <Integer, List<String >> nodesToPluckOut = loabBalancingHueristic.nodesToPluckOut;
                Map <Integer, Integer> nodesToMoveIn =loabBalancingHueristic.nodesToMoveIn;



                //STEP 6 : 
                //*****************************************************************************************************************            
                //farm out nodes. This time we fetch the whole node attachment , not just metadata about the node.
                //Key is the partition id
                Map <Integer, List<NodeAttachment > > farmedNodes = new HashMap <Integer,List<NodeAttachment > > ();
                JavaPairRDD<Integer, List<NodeAttachment >> farmedNodesRDD = filteredFrontier.mapPartitionsToPair(new NodePlucker (  nodesToPluckOut) , true);
                farmedNodes = farmedNodesRDD.collectAsMap();

                //STEP 7 : 
                //*****************************************************************************************************************            
                //recreate farmed nodes on new partitions
                //nodesToMoveIn tells us how many to move to each destination partition
                List<Tuple2<Integer,NodeAttachment>> newPairs = new ArrayList<Tuple2<Integer,NodeAttachment>> ();

                for (Entry<Integer, Integer> entry : nodesToMoveIn.entrySet()){
                    int partitionID = entry.getKey();
                    int count = entry.getValue();

                    //get count nodes from farmedNodes, and move them into partition partitionID
                    //This process is similar to what we did at the outset with the original problem
                    newPairs .addAll( getSubSetOfFarmedNodes(farmedNodes, count, partitionID)) ;                
                }
                JavaRDD < Tuple2<Integer,NodeAttachment>> migratedNodesRDD   = sc.parallelize(newPairs);
                JavaPairRDD < Integer,ActiveSubtree> migratedNodesPairRDD = migratedNodesRDD.mapToPair(new TupleToPairConverter())
                                                                            /*realize the movement to desired partition*/
                                                                            .partitionBy(new HashPartitioner(NUM_AVAILABLE_CORES)) 
                                                                            /* convert the attachment into an ActiveSubTree object*/
                                                                            .mapValues( new AttachmentConverter() ); 

                //STEP 8 : 
                //*****************************************************************************************************************
                //merge migrated nodes with existing frontier
                frontier = filteredFrontier.union(      migratedNodesPairRDD);   
                
            } //end if perPartitionPendingNodesMap is empty
                    
            
            //STEP 9 : 
            //*****************************************************************************************************************
            // update perPartitionPendingNodesMap by recounting number of kids pending on each tree.
            
            perPartitionPendingNodesRDD =  frontier.mapPartitionsToPair(new NodeMetaDataFetcher(), true);
            //no need to cache the RDD as it is used only once
            perPartitionPendingNodesMap= perPartitionPendingNodesRDD.collectAsMap();
            
            //STEP 10 :         
            //do the next iteration
                    
            
        } //end driver iterations
        
        //
        logger.info("Sparcplex V3 completed in "+  iteration +" iterations." ) ;
        if (isSolutionHalted) {
             logger.info("Solution was halted "  ); 
        } else {
            String status = "Infeasible";
            if (incumbent.isFeasible()) status = "Feasible";
            if (incumbent.isOptimal()) status = "optimal.";            
            if (incumbent.isError()) status = "error.";       
            if (incumbent.isUnbounded()) status = "unbounded.";       
            logger.info("Solution status is " + status); 
            if (incumbent.isFeasibleOrOptimal()) {
                logger.info( incumbent ); 
            } 
        }
        
         
    }//end main
    
    private static Instant getIterationEndTime ( Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap) {   
        double timeSlice = ZERO;
       
        //calculate the time slice using node meta data of each partition 
        timeSlice = ITERATION_TIME_MAX_SECONDS;
            
        return      Instant.now().plusMillis(THOUSAND*(int)timeSlice );
        
    } 
    
    //extract count nodes from available farmed out nodes
    private static  List<Tuple2<Integer,NodeAttachment>>  getSubSetOfFarmedNodes(Map <Integer, List<NodeAttachment > >  farmedNodes, int count, int partitionID ) {
        List<Tuple2<Integer,NodeAttachment>> retval = new ArrayList<Tuple2<Integer,NodeAttachment>> ();
        
        while (count > ZERO) {
            NodeAttachment node =removeOneNodeFromMap(farmedNodes);
            Tuple2<Integer,NodeAttachment> tuple = new Tuple2<Integer,NodeAttachment>(partitionID, node);
            retval.add(tuple);
            
            count = count - ONE;
        }
        
        return retval;
    }
    
    private static NodeAttachment removeOneNodeFromMap (Map <Integer, List<NodeAttachment > >  farmedNodes) {
        NodeAttachment node =null;
        
        List<NodeAttachment > nodeList = null;
        int pid = ZERO;
        
        for (Entry <Integer, List<NodeAttachment >> entry :farmedNodes.entrySet() ) {
            nodeList= entry.getValue();
            node=nodeList.remove(ZERO);      
            pid = entry.getKey();
        }
        if (nodeList.size()>ZERO) {
            farmedNodes.put(pid, nodeList);
        }else {
            farmedNodes.remove(pid);
        }
        
        return node;
    }
    
}//end driver class

