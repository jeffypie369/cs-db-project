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

public class SortMergeJoin extends Join {

    private Operator table;

    ObjectInputStream inputLeft;
    ObjectInputStream inputRight;

    private String leftTableName = "LSTTemp-";
    private String rightTableName = "RSTTemp-";

    private int leftAttrIndex;
    private int rightAttrIndex;

    private ExternalSort leftTable;
    private ExternalSort rightTable;
    int tupleSize, batchSize, rightTupleSize, leftTupleSize, leftBatchSize, rightBatchSize;


    private int leftTablePointer;
    private int rightTablePointer;

    private Batch leftBatch;
    private ArrayList<Batch> rightBuffer;

    private Tuple leftTuple;
    private Tuple rightTuple;

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

        // Number of tuples per batch
        tupleSize = getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        leftTupleSize = getLeft().getSchema().getTupleSize();
        rightTupleSize = getRight().getSchema().getTupleSize();

        leftBatchSize = Batch.getPageSize() / leftTupleSize;
        rightBatchSize = Batch.getPageSize() / rightTupleSize;

        leftBatch = new Batch(leftBatchSize);
        rightBuffer = new ArrayList<>();

        Attribute leftAttr = getCondition().getLhs();
        Attribute rightAttr = (Attribute) getCondition().getRhs();

        leftAttrIndex = getLeft().getSchema().indexOf(leftAttr);
        rightAttrIndex = getRight().getSchema().indexOf(rightAttr);

        currentLeftBatchIndex = 0;
        currentRightBufferIteration = 0;
        leftTablePointer = 0;
        rightTablePointer = 0;

        leftTable = new ExternalSort(left, numBuff, leftAttrIndex, leftTableName);
        rightTable = new ExternalSort(right, numBuff, rightAttrIndex, rightTableName);


        if (!leftTable.open() || !rightTable.open()) {
            System.err.println("SortMergeJoin:Error in opening tables");
            return false;
        }

        try {
            inputLeft = new ObjectInputStream(new FileInputStream(leftTableName + "0.0"));
            inputRight = new ObjectInputStream(new FileInputStream(rightTableName + "0.0"));
        } catch (IOException io) {
            System.err.println("SortMergeJoin:Error in reading in input files");
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

        Tuple rightTuple = loadNextRightTuple(rightTablePointer);
        Tuple leftTuple = loadNextLeftTuple(leftTablePointer);

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
                rightTuple = loadNextRightTuple(rightTablePointer);
                leftTuple = loadNextLeftTuple(leftTablePointer);
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

        if (comparison > 0) {
            return true;
        }
        return false;
    }

    private boolean rightIsGreaterThanLeft(Tuple leftTuple, Tuple rightTuple) {

        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);

        if (comparison < 0) {
            return true;
        }

        return false;
    }

    private boolean leftIsEqualToRight(Tuple leftTuple, Tuple rightTuple) {

        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);

        if (comparison == 0) {
            return true;
        }

        return false;
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

        batchIndex = index / leftBatchSize;
        tupleIndexInBatch = index % leftBatchSize;

        System.out.println("left: " + index + ", " + leftBatchSize + ", " + batchIndex + ", " + currentLeftBatchIndex);
        Batch output = readLeftBatch(batchIndex);
        leftBatch = output;

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
        bufferIteration = batchIndex / (numBuff - 2);

        if (bufferIteration != currentRightBufferIteration) {
            currentRightBufferIteration++;
        }
        System.out.println("right " + index + ", " + rightBatchSize + ", " + batchIndex + ", " + bufferIteration + ", " + currentRightBufferIteration);
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
                Batch leftIntermediateBatch = (Batch) inputLeft.readObject();
                output = leftIntermediateBatch;
            } catch (EOFException eof) {

                try {
                    inputLeft.close();
                    File f = new File(leftTableName + "0.0");
                    f.delete();
                } catch (IOException io) {
                    System.err.println("SortMergeJoin: Error in reading left batches" + io);
                    System.exit(1);
                }
            } catch (ClassNotFoundException ce) {
                System.err.println("SortMergeJoin: Error in reading left batches" + ce);
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
                        System.err.println("SortMergeJoin: Error in reading right buffer" + io);
                        System.exit(1);
                    }
                } catch (ClassNotFoundException ce) {
                    System.err.println("SortMergeJoin: Error in reading right buffer" + ce);
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
                File f = new File(leftTableName + "0.0");
                f.delete();
            } catch (IOException io) {
                System.err.println("SortMergeJoin: Error in reading left batches" + io);
                System.exit(1);
            }
        } catch (ClassNotFoundException ce) {
            System.err.println("SortMergeJoin: Error in reading left batches" + ce);
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
                System.out.println(rightBuffer);
            } catch (EOFException eof) {
                try {
                    inputRight.close();
                    File f = new File(rightTableName + "0.0");
                    f.delete();
                } catch (IOException io) {
                    System.err.println("SortMergeJoin: Error in reading right buffer" + io);
                    System.exit(1);
                }
            } catch (ClassNotFoundException ce) {
                System.err.println("SortMergeJoin: Error in reading right buffer" + ce);
                System.exit(1);
            } catch (IOException io) {
                return;
            }
        }
    }
}
