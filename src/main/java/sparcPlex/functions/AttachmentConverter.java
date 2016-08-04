package  sparcPlex.functions;

import cplexLib.dataTypes.ActiveSubtree;
import cplexLib.dataTypes.NodeAttachment;
import java.io.Serializable;
import org.apache.spark.api.java.function.Function; 

public class AttachmentConverter implements Function <NodeAttachment, ActiveSubtree>, Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ActiveSubtree call(NodeAttachment node) throws Exception {
         
        return new ActiveSubtree(node) ;
    }

}
