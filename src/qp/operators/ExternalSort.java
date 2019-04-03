package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Stack;

import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * 2 phases to External sort -
 * Phase 1 - Create ceiling (no of pages/no of batches) sorted runs (Generates a (buffer size) sorted run)
 * Phase 2 - Merge the runs together (Merge using B-1 buffer pages until 1 sorted run is produced)
 */
public class ExternalSort extends Operator{

    private Operator table;
    private Stack fileStack;
    private ArrayList<Tuple> memory;

    private String fileName;

    private int numBuff; //number of buffers
    private double runNum = 0;
    private int batchSize; // number of tuples in one page
    private int joinIndex;
    private int numPasses; // number of passes
    private Batch batch;
    private boolean flag;

    public ExternalSort(Operator table, int numBuff, int joinIndex, String fileName ) {
        super(OpType.SORT);
        this.table = table;
        this.numBuff = numBuff;
        this.joinIndex = joinIndex;
        this.fileName = fileName;
    }

    public boolean open() {
        if (!table.open()) {
            return false;
        }

        memory = new ArrayList<>();
        fileStack = new Stack();
        phaseOne(); //Create sorted runs
        flag = true;
        phaseTwo(); //Merge Sorted Runs

        return true;
    }

    /**
     * Generate sorted runs
     */
    private void phaseOne() {

        if (runNum == 0) {
            batch = table.next();

            if (batch != null) {
                batchSize = batch.size();
            }
        }

        while (batch != null) {

            if (batch == null && memory.size() == 0) {
                break;
            }
            loadTuplesIntoMemory();
            sortRuns();
            writeRunsToFile(runNum); // each run is assigned a number
            runNum++;
        }
    }

    private void loadTuplesIntoMemory() {

        for (int i = 0;i < numBuff;i++) {

            if (batch != null) {
                for (int j = 0;j < batch.size();j++) {
                    memory.add(batch.elementAt(j));
                }
            }
            else {
                return;
            }
            batch = table.next();
        }
    }

    /**
     * Internal sort the runs
     */
    private void sortRuns() {
        Collections.sort(memory, (leftTuple,rightTuple) -> Tuple.compareTuples(leftTuple,rightTuple,joinIndex));
    }

    /**
     * writeSortedRuns transfers whatever data in the memory to a new file, differentiated by the current run.
     * Output is the batch that transfers over the data from memory to file. After each batch is taken, the memory erases that
     * particular batch.
     * @param currentRun
     */

    private void writeRunsToFile(double currentRun){

        if (flag == false) {
            fileStack.push(currentRun);
        }

        try {
            String currentFileName = fileName + String.valueOf(currentRun);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(currentFileName));

            while(!memory.isEmpty()) { // still have tuples in memory
                Batch output = new Batch(batchSize); // write existing tuples to a new batch

                while(!output.isFull()) {
                    if (memory.isEmpty()) {
                        break;
                    }
                    output.add(memory.get(0)); // while the existing batch is not full, write the tuples into it
                    memory.remove(0);

                }
                out.writeObject(output);
            }
            out.close();
        } catch (IOException io) {
            System.err.println("External Sort: Error in writing file");
            System.exit(1); // Terminates the system
        }
    }

    private void phaseTwo() {
        numPasses = calculateNumPasses();
        mergeRuns(); // as explained in lecture slides
        close();
    }

    private int calculateNumPasses() {
        int num = 0;
        double numInputBuffers = numBuff - 1;
        double numRuns;
        numRuns = runNum;


        while (numRuns!= 1) {
            numRuns = Math.ceil(numRuns/numInputBuffers);
            num++;
        }

        return num;
    }

    private void mergeRuns() {

        double numInputBuffers = numBuff - 1;
        int readRunIntoMemoryPointer = 0;
        int writeRunIntoFilePointer = 0;

        /**
         * runNum slowly decreases with increasing passes. Runs become bigger.
         */
        while (runNum != 1) {
            for (int i = 0; i < numPasses;i++) {

                for(int j = 0;j < numInputBuffers;j++) {
                    if (!readRunIntoMemory(readRunIntoMemoryPointer)) {
                        break;
                    }
                    readRunIntoMemoryPointer++;
                }
                sortRuns();
                writeRunsToFile(writeRunIntoFilePointer);
                writeRunIntoFilePointer++;
            }
            readRunIntoMemoryPointer = 0; // reset
            writeRunIntoFilePointer = 0; // reset
            runNum = Math.ceil(runNum/numInputBuffers);
            close();
        }

    }

    private boolean readRunIntoMemory(int run) {
        ObjectInputStream in = null;
        try {
            String currentFile = fileName + String.valueOf(run);
            in = new ObjectInputStream(new FileInputStream(currentFile));

            Batch input;
            input = (Batch) in.readObject();

            while (input != null) {

                for (int i = 0;i < input.size();i++) {
                    memory.add(input.elementAt(i)); // read tuples in batch into memory
                }
                input = (Batch) in.readObject(); // read next batch
            }
        } catch (EOFException e) {
            try {
                in.close();
                return true;
            } catch (IOException io) {
                System.exit(1);
            }
        } catch (ClassNotFoundException ce) {
            System.err.println("External Sort: Class type not Batch when reading in file");
            System.exit(1);
        }
        catch (IOException io) {
            System.err.println("External Sort:Error in reading file");
            System.exit(1);
            return false;
        }
        return true;
    }

    public boolean close() {
        while((double) fileStack.peek() != runNum - 1) {
            System.out.println(fileName);
            File f = new File(fileName + (int) fileStack.peek());
            f.delete();
            fileStack.pop();
        }
        return true;
    }
}

