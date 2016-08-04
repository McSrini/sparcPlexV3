/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.functions;
 
import cplexLib.dataTypes.NodeAttachment;
import java.io.Serializable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author srini
 */
 
public class TupleToPairConverter implements PairFunction<Tuple2<Integer,NodeAttachment>,Integer,NodeAttachment>, Serializable{
 
    public Tuple2<Integer, NodeAttachment> call(Tuple2<Integer, NodeAttachment> tuple) throws Exception {
        int partitionID = tuple._1;
        NodeAttachment node = tuple._2;
        Tuple2<Integer, NodeAttachment> newTuple = new Tuple2<Integer, NodeAttachment>(partitionID, node);
        return newTuple;
    }

  
        
}
