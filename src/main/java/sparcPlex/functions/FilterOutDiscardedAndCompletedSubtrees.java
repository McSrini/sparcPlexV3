 package  sparcPlex.functions;

import cplexLib.dataTypes.ActiveSubtree;
import java.io.Serializable;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2; 

public class FilterOutDiscardedAndCompletedSubtrees implements Function<Tuple2<Integer, ActiveSubtree>, Boolean>,Serializable{

    public Boolean call(Tuple2<Integer, ActiveSubtree> tuple) throws Exception {
        ActiveSubtree subTree =  tuple._2;
        if (subTree.isDiscardable()) subTree.end();
        if (subTree.isSolvedToCompletion()) subTree.end(); 
        return ! subTree.isDiscardable() && ! subTree.isSolvedToCompletion();
    }

}
