package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join{

    private ObjectInputStream inputLeft;
    private ObjectInputStream inputRight;

    private String leftTableName = "LSTTemp-";
    private String rightTableName = "RSTTemp-";

    private int leftAttrIndex;
    private int rightAttrIndex;


    private int batchSize, leftBatchSize, rightBatchSize;


    private int leftTablePointer;
    private int rightTablePointer;

    private Batch leftBatch;
    private ArrayList<Batch> rightBuffer;

    private int currentLeftBatchIndex;
    private int currentRightBufferIteration;

    /**
     * 2 phases to External sort -
     * Phase 1 - Create ceiling (no of pages/no of batches) sorted runs (Generates a (buffer size) sorted run)
     * Phase 2 - Merge the runs together (Merge using B-1 buffer pages until 1 sorted run is produced)
     */
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        int tupleSize, leftTupleSize, rightTupleSize;
        ExternalSort leftTable;
        ExternalSort rightTable;

        // Number of tuples per batch
        tupleSize = getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        leftTupleSize = getLeft().getSchema().getTupleSize();
        rightTupleSize = getRight().getSchema().getTupleSize();

        leftBatchSize = Batch.getPageSize() / leftTupleSize; // no of tuples in 1 left batch
        rightBatchSize = Batch.getPageSize() / rightTupleSize; // no of tuples in 1 right batch

        // to extract tuples from the 1 left sorted table file and 1 right sorted table file, have to make use of buffers
        // to extract the tuples via batches
        leftBatch = new Batch(leftBatchSize); // just 1 buffer for left table
        rightBuffer = new ArrayList<>(); // assign numBuff - 2 to right Buffer to accomodate large right table

        Attribute leftAttr = getCondition().getLhs();
        Attribute rightAttr = (Attribute) getCondition().getRhs();

        leftAttrIndex = getLeft().getSchema().indexOf(leftAttr);
        rightAttrIndex = getRight().getSchema().indexOf(rightAttr);

        currentLeftBatchIndex = 0; // keeps track of the ith left batch index
        currentRightBufferIteration = 0; // keeps track of the ith right buffer iteration
        leftTablePointer = 0;
        rightTablePointer = 0;

        leftTable = new ExternalSort(left, numBuff, leftAttrIndex, leftTableName);
        rightTable = new ExternalSort(right, numBuff, rightAttrIndex, rightTableName);


        if (!leftTable.open() || !rightTable.open()) {
            System.err.println("smj:Error in opening tables");
            return false;
        }

        // file name ends with 0.0 (have to double check with what is created in directory
        try {
            inputLeft = new ObjectInputStream(new FileInputStream(leftTableName + "0.0"));
            inputRight = new ObjectInputStream(new FileInputStream(rightTableName + "0.0"));
        } catch (IOException io) {
            System.err.println("smj:Error in reading in input files");
            System.exit(1);
        }

        initialize();
        return true;
    }

    /**
     * When left table tuples matches with right table tuples, output the result pair to a batch
     *
     * @return a batch of tuple result pair
     */
    public Batch next() {

        Batch outBatch = new Batch(batchSize);

        if (endOfLeftTable() || endOfRightTable()) {

        } else {
            Tuple rightTuple = loadNextRightTuple(rightTablePointer);
            Tuple leftTuple = loadNextLeftTuple(leftTablePointer);

            // always check for EOF
            while (!(outBatch.isFull() || endOfLeftTable() || endOfRightTable())) {

                if (leftIsGreaterThanRight(leftTuple, rightTuple)) {
                    rightTablePointer++;

                    if (endOfRightTable()) {
                        break;
                    }
                    loadNextRightTuple(rightTablePointer);
                } else if (rightIsGreaterThanLeft(leftTuple, rightTuple)) {
                    leftTablePointer++;

                    if (endOfLeftTable()) {
                        break;
                    }
                    loadNextLeftTuple(leftTablePointer);
                } else {

                    combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
                    rightTablePointer++;

                    if (endOfRightTable()) {
                        break;
                    }
                    loadNextRightTuple(rightTablePointer);

                    while (!endOfRightTable() && !endOfLeftTable() && leftIsEqualToRight(leftTuple, rightTuple)) {

                        combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
                        rightTablePointer++;

                        if (endOfRightTable()) {
                            break;
                        }

                        loadNextRightTuple(rightTablePointer);
                    }

                    leftTablePointer++;
                    loadNextLeftTuple(leftTablePointer);
                    while (!endOfRightTable() && !endOfLeftTable() && leftIsEqualToRight(leftTuple, rightTuple)) {

                        combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
                        leftTablePointer++;

                        if (endOfLeftTable()) {
                            break;
                        }

                        loadNextLeftTuple(leftTablePointer);
                    }

                    leftTablePointer++;
                    rightTablePointer++;

                    if (endOfRightTable()) {
                        break;
                    }

                    if (endOfLeftTable()) {
                        break;
                    }

                    rightTuple = loadNextRightTuple(rightTablePointer);
                    leftTuple = loadNextLeftTuple(leftTablePointer);
                }
            }

        }

        if (outBatch.isEmpty()) {
            return null;
        } else {
            return outBatch;
        }
    }

    public boolean close() {
        return (left.close() && right.close());
    }

    private boolean leftIsGreaterThanRight(Tuple leftTuple, Tuple rightTuple) {
        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);

        return comparison > 0;
    }

    private boolean rightIsGreaterThanLeft(Tuple leftTuple, Tuple rightTuple) {

        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);

        return comparison < 0;

    }

    private boolean leftIsEqualToRight(Tuple leftTuple, Tuple rightTuple) {

        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);

        return comparison == 0;

    }

    private void combineTuplesIntoOutput(Tuple leftTuple, Tuple rightTuple, Batch outBatch) {
        Tuple combine = leftTuple.joinWith(rightTuple);
        outBatch.add(combine);
    }

    private boolean endOfLeftTable() {

        try {
            loadNextLeftTuple(leftTablePointer);
            return false;
        } catch (IndexOutOfBoundsException ie) {
            return true;
        }
    }

    private boolean endOfRightTable() {

        try {
            loadNextRightTuple(rightTablePointer);
            return false;
        } catch (IndexOutOfBoundsException ie) {
            return true;
        }
    }

    private Tuple loadNextLeftTuple(int index) {
        int batchIndex, tupleIndexInBatch;

        batchIndex = index / leftBatchSize; // access the ith batch that contains the tuple
        tupleIndexInBatch = index % leftBatchSize; // access the exact location of the tuple in the batch

        leftBatch = readLeftBatch(batchIndex);

        if (batchIndex != currentLeftBatchIndex) {
            currentLeftBatchIndex++;
        }
        if (leftBatch == null) {
            return null;
        }

        return leftBatch.elementAt(tupleIndexInBatch);
    }

    private Tuple loadNextRightTuple(int index) {
        int batchIndex, tupleIndexInBatch, bufferIteration;

        batchIndex = index / rightBatchSize;


        tupleIndexInBatch = index % rightBatchSize;
        bufferIteration = batchIndex / (numBuff - 2); // a number of buffers exist in 1 iteration.

        if (bufferIteration != currentRightBufferIteration) {
            currentRightBufferIteration++;
        }

        Batch output = readRightBuffer(batchIndex, bufferIteration); // outputs the buffer in the array of buffers(batches)

        if (output == null) {
            return null;
        }

        return output.elementAt(tupleIndexInBatch);
    }

    private Batch readLeftBatch(int batchIndex) {
        Batch output = new Batch(leftBatchSize);

        if (currentLeftBatchIndex == batchIndex) {
            return leftBatch;
        } else {

            try {
                output = (Batch) inputLeft.readObject();
            } catch (EOFException eof) {

                try {
                    inputLeft.close();
                    File inputFile = new File(leftTableName + "0.0");
                    inputFile.delete();
                } catch (IOException io) {
                    System.err.println("smj: Error in reading left batches" + io);
                    System.exit(1);
                }
            } catch (ClassNotFoundException ce) {
                System.err.println("smj: Error in reading left batches" + ce);
                System.exit(1);
            } catch (IOException io) {
                return null;
            }
        }
        return output;
    }

    private Batch readRightBuffer(int batchIndex, int bufferIteration) {
        Batch output;
        if (bufferIteration == currentRightBufferIteration) {
            return rightBuffer.get(batchIndex % (numBuff - 2));
        } else {
            rightBuffer.clear();
            for (int i = 0;i < (numBuff - 2); i++) {
                try {
                    Batch batch = (Batch) inputRight.readObject();

                    if (batch == null) {
                        break;
                    }
                    rightBuffer.add(batch);
                    System.out.println(rightBuffer);
                } catch (EOFException eof) {
                    try {
                        inputRight.close();
                        File f = new File(rightTableName + "0.0");
                        f.delete();
                    } catch (IOException io) {
                        System.err.println("smj: Error in reading right buffer" + io);
                        System.exit(1);
                    }
                } catch (ClassNotFoundException ce) {
                    System.err.println("smj: Error in reading right buffer" + ce);
                    System.exit(1);
                } catch (IOException io) {
                    return null;
                }
            }
            currentRightBufferIteration++;
            output = rightBuffer.get(batchIndex % (numBuff - 2));
        }

        return output;
    }

    /**
     * initialize buffers before proceeding with merging
     */
    private void initialize() {
        initializeLeftBatch();
        initializeRightBuffer();
    }

    private void initializeLeftBatch() {

        try {
            leftBatch = (Batch) inputLeft.readObject();

            if (leftBatch == null) {
                return;
            }
        } catch (EOFException eof) {

            try {
                inputLeft.close();
                File inputFile  = new File(leftTableName + "0.0");
                inputFile.delete();
            } catch (IOException io) {
                System.err.println("smj: Error in reading left batches" + io);
                System.exit(1);
            }
        } catch (ClassNotFoundException ce) {
            System.err.println("smj: Error in reading left batches" + ce);
            System.exit(1);
        } catch (IOException io) {
            return;
        }
    }

    private void initializeRightBuffer() {
        for (int i = 0;i < (numBuff - 2); i++) {
            try {
                Batch batch = (Batch) inputRight.readObject();

                if (batch == null) {
                    break;
                }
                rightBuffer.add(batch);
            } catch (EOFException eof) {
                try {
                    inputRight.close();
                    File inputFile = new File(rightTableName + "0.0");
                    inputFile.delete();
                } catch (IOException io) {
                    System.err.println("smj: Error in reading right buffer" + io);
                    System.exit(1);
                }
            } catch (ClassNotFoundException ce) {
                System.err.println("smj: Error in reading right buffer" + ce);
                System.exit(1);
            } catch (IOException io) {
                return;
            }
        }
    }
}
