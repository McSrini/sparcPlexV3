/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.constsAndParams;

import static cplexLib.constantsAndParams.Constants.*;
import java.io.Serializable;

/**
 *
 * @author srini
 */
public class Parameters implements Serializable{
    
    public  static final  int CORES_PER_MACHINE = TWO*TWO;
    public  static final  int NUM_MACHINES= TEN;
    public  static final  int NUM_AVAILABLE_CORES = -ONE + CORES_PER_MACHINE * NUM_MACHINES ; //  1 core will be taken up by the driver
    
    public  static final  double EASY_NODE_TIME_SLICE_SECONDS =     SIX; 
    public  static final  double HARD_NODE_TIME_SLICE_SECONDS =    EASY_NODE_TIME_SLICE_SECONDS* TEN ;
      
    public  static final  double   ITERATION_TIME_MAX_SECONDS =    HARD_NODE_TIME_SLICE_SECONDS*TEN ; 
    public  static final  double ITERATION_TIME_MIN_SECONDS =   ITERATION_TIME_MAX_SECONDS/TWO ; 
        
    public static final int MIN_LEAFS_PER_PARTITION = TEN;
}
