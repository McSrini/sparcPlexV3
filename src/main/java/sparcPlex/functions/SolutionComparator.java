package  sparcPlex.functions; 

import static cplexLib.constantsAndParams.Constants.*;
import cplexLib.dataTypes.Solution;
import java.io.Serializable;
import java.util.*;

import static sparcPlex.constsAndParams.Constants.*;
import static sparcPlex.constsAndParams.Parameters.*;
  
public class SolutionComparator implements Comparator<  Solution > , Serializable {
 
    /**
     * 
     */
    private static final long serialVersionUID = 6312143274435503803L;

    //return 1 if soln2 is better, else return 0
    //
    //if soln2 is unbounded or in error, we return 1. 
    //Caller should check status and abort computation, in case of error/unbounded.
    //
    public int compare(Solution soln1, Solution soln2) {

        int retval = ZERO;
        
        if (soln2.isError() || soln2.isUnbounded())    {
             retval=ONE;
        } else if (soln2.isFeasibleOrOptimal())    {
            if (soln1.isFeasibleOrOptimal()) {
                if (  isMaximization &&  soln1.getObjectiveValue() < soln2.getObjectiveValue()  ) retval = ONE;
                if (!  isMaximization &&  soln1.getObjectiveValue() > soln2.getObjectiveValue()  ) retval = ONE;
            }else{
                retval=ONE;
            }             
        } 
        
        return retval;
        
    }
  
}
