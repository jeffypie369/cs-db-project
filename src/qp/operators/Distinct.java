/*
  To projec out the required attributes from the result
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.*;

public class Distinct extends Operator {

    Operator base;
    Vector attrSet;
    int batchsize;  // number of tuples per outbatch

    /**
     * The following fields are requied during execution
     * * of the Distinct Operator
     **/

    Batch inBatch, outBatch;

    /**
     * index of the attributes in the base operator
     * * that are to be distinctly projected
     **/

    int[] attrIndex;
    Vector<Tuple> tableContent = new Vector<>();
    boolean readComplete = false;
    int globalReadIndex = 0;


    public Distinct(Operator base, Vector as, int type) {
        super(type);
        this.base = base;
        this.attrSet = as;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public Vector getProjAttr() {
        return attrSet;
    }

    void printTuple(Tuple t){
        for(int i=0;i<attrIndex.length;i++){
            Object data = t.dataAt(i);
            if(data instanceof Integer){
                System.out.print(((Integer)data).intValue()+"\t");
            }else if(data instanceof Float){
                System.out.print(((Float)data).floatValue()+"\t");
            }else{
                System.out.print(((String)data)+"\t");
            }
        }
        System.out.println();
    }

    private boolean sortTableScan(Vector<Tuple> v) {
        //Bubblesort right now
       /*boolean swapped = true;
        while(swapped) {
            swapped = false;
            for(int i = 0; i < v.size() - 1; i++){
                for(int j = 0; j < attrIndex.length; j++) {
                    int cmpResult = Tuple.compareTuples(v.elementAt(i), v.elementAt(i+1), attrIndex[j]);
                    if (cmpResult > 0) {
                        swapped = true;
                        Collections.swap(v,i, i+1);
                        break;
                    }
                    if (cmpResult < 0) {
                        break;
                    }
                }
            }
        }*/

        //for(int i = 0; i < v.size(); i++){
        //    printTuple(v.elementAt(i));
        //}
        //CollectionSort is better
        v.sort( (arg0, arg1)
                        ->{
                    boolean toContinue = false;
                    for(int j = 0; j < attrIndex.length; j++){
                        int cmpResult = Tuple.compareTuples(arg0, arg1, attrIndex[j]);
                        if(cmpResult != 0){ return cmpResult; }
                    }
                    return 0;
                }
        );

        return true;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * ProjectDuplicateEliminationed from the base operator
     **/

    @Override
    public boolean open(){
        /** setnumber of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize=Batch.getPageSize()/tuplesize;


        /** The followingl loop findouts the index of the columns that
         ** are required from the base operator
         **/

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];
        //System.out.println("Distinct---Schema: ----------in open-----------");
        //System.out.println("base Schema---------------");
        //Debug.PPrint(baseSchema);
        for(int i=0;i<attrSet.size();i++){
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i]=index;

            //  Debug.PPrint(attr);
            //System.out.println("  "+index+"  ");
        }

        if(!base.open()){ return false; }

        Batch tmp = base.next();
        while(tmp != null){
            for(int i = 0; i < tmp.size(); i++){
                tableContent.add(tmp.elementAt(i));
            }
            tmp = base.next();
        }

        sortTableScan(tableContent);
        return true;
    }


    private Batch tableScanNext() {
        Batch nextBatch = new Batch(batchsize);

        while(true){
            if(globalReadIndex >= tableContent.size()){
                readComplete = true;
                return nextBatch;
            }
            Tuple newT = tableContent.get(globalReadIndex);
            globalReadIndex++;
            nextBatch.add(newT);
            if(nextBatch.isFull()){ return nextBatch; }
        }
    }

    /**
     * Read next tuple from operator
     */

    private boolean initializeNextBatch(){
        return (inBatch = tableScanNext()) == null;
    }

    @Override
    public Batch next() {
        //System.out.println("Distinct:-----------------in next-----------------");

        Vector<Tuple> lEntry = new Vector<>();
        outBatch = new Batch(batchsize);

        if(outBatch.isFull()){  /*System.out.println("Outbatch1: " + outBatch.size());*/ return outBatch; }
        while(true){ //We have to scan through all batches because some tuples may get skipped
            if(readComplete){  /*System.out.println("Outbatch2: null");*/ return null; }
            if(initializeNextBatch()){  /*System.out.println("Outbatch3: " + outBatch.size());*/ return outBatch; }
            /*System.out.println("iteration: " + inBatch.size() + ", " + outBatch.capacity());*/

            //System.out.println("Distinct:---------------base tuples---------");
            for(int i = 0; i < inBatch.size(); i++) {
                Tuple basetuple = inBatch.elementAt(i);
                //Debug.PPrint(basetuple);
                //System.out.println();
                Vector present = new Vector();
                for(int j=0;j<attrSet.size();j++){
                    Object data = basetuple.dataAt(attrIndex[j]);
                    present.add(data);
                }
                //printTuple(new Tuple(present));
                /*System.out.println("batchsize: " + outBatch.size() + ", " + outBatch.capacity());*/
                if (!lEntry.equals(present)) { //Check for two subsequent elements
                    Tuple outtuple = new Tuple(present);
                    //printTuple(outtuple);
                    outBatch.add(outtuple);
                    lEntry = present;
                    if(outBatch.isFull()){ /*System.out.println("Outbatch4: " + outBatch.size() + ", " + outBatch.capacity());*/ return outBatch; }
                }
            }
            if(readComplete){ return outBatch; } //Important to return batch, if file empty but batch not full
        }
    }


    /** Close the operator */
    public boolean close(){
        return true;
		/*
	if(base.close())
	    return true;
	else
	    return false;
	    **/
    }



    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for (int i = 0; i < attrSet.size(); i++)
            newattr.add((Attribute) ((Attribute) attrSet.elementAt(i)).clone());
        Distinct newproj = new Distinct(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
}
