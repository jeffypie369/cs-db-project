package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.PriorityQueue;
import java.util.Stack;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 *  Test class for sort merge join. To be deleted after testing
 */
public class SMJ extends Join {


    private int batchSize;

    private Stack<Tuple> tupleStack; // Stack to store deleted values for duplicate handling

    private int leftAttrIndex;
    private int rightAttrIndex;

    private String leftTableName = "LSTTemp-";
    private String rightTableName = "RSTTemp-";

    private ExternalSort leftTable;
    private ExternalSort rightTable;
    int tupleSize, rightTupleSize, leftTupleSize, leftBatchSize, rightBatchSize;

    ObjectInputStream inputLeft;
    ObjectInputStream inputRight;

    private int leftTablePointer;
    private int rightTablePointer;

    private PriorityQueue<Tuple> leftPQ;
    private PriorityQueue<Tuple> rightPQ;

    private boolean hasLoadLastLeftBlock;
    private boolean hasLoadLastRightBatch;

    private boolean hasFinishLeftRelation;
    private boolean hasFinishRightRelation;

    private boolean isFirstBatch = true;
    private boolean isFirstBlock = true;

    private Tuple leftTuple;
    private Tuple rightTuple;

    private Batch outputBatch;

    private int currentLeftBatchIndex;
    private int currentRightBatchIndex;

    public SMJ(Join jn) {
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

        Attribute leftAttr = getCondition().getLhs();
        Attribute rightAttr = (Attribute) getCondition().getRhs();

        leftAttrIndex = getLeft().getSchema().indexOf(leftAttr);
        rightAttrIndex = getRight().getSchema().indexOf(rightAttr);

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

        leftPQ = new PriorityQueue<>((t1, t2) -> Tuple.compareTuples(t1, t2, leftAttrIndex));
        rightPQ = new PriorityQueue<>((t1, t2) -> Tuple.compareTuples(t1, t2, rightAttrIndex));

        tupleStack = new Stack<>();

        return true;
    }

    /**
     * When left table tuples matches with right table tuples, output the result pair to a batch
     *
     * @return a batch of tuple result pair
     */
//    public Batch next() {
//
//        Batch outBatch = new Batch(batchSize);
//
//        Tuple rightTuple = loadNextRightTuple(rightTablePointer);
//        Tuple leftTuple = loadNextLeftTuple(leftTablePointer);
//
//        while (!(outBatch.isFull() || endOfLeftTable() || endOfRightTable())) {
//
//            if (leftIsGreaterThanRight(leftTuple, rightTuple)) {
//                rightTablePointer++;
//                loadNextRightTuple(rightTablePointer);
//            } else if (rightIsGreaterThanLeft(leftTuple, rightTuple)) {
//                leftTablePointer++;
//                loadNextLeftTuple(leftTablePointer);
//            } else {
//
//                combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
//                rightTablePointer++;
//                loadNextRightTuple(rightTablePointer);
//
//                while (!endOfRightTable() && leftIsEqualToRight(leftTuple, rightTuple)) {
//
//                    combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
//                    rightTablePointer++;
//                    loadNextRightTuple(rightTablePointer);
//                }
//
//                leftTablePointer++;
//                loadNextLeftTuple(leftTablePointer);
//                while (!endOfLeftTable() && leftIsEqualToRight(leftTuple, rightTuple)) {
//
//                    combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
//                    leftTablePointer++;
//                    loadNextLeftTuple(leftTablePointer);
//                }
//
//                leftTablePointer++;
//                rightTablePointer++;
//                rightTuple = loadNextRightTuple(rightTablePointer);
//                leftTuple = loadNextLeftTuple(leftTablePointer);
//            }
//        }
//        if (outBatch.isEmpty()) {
//            return null;
//        } else {
//            return outBatch;
//        }
//    }

    public Batch next() {

        if (hasFinishRightRelation || hasFinishLeftRelation) {
            return null;
        }

        outputBatch = new Batch(batchSize);
        while (!outputBatch.isFull()) { // output batch is not full
            if (!hasLoadLastLeftBlock) {
                loadLeftBlock(); // load blocks from left relation
            }

            /** leftPQ is empty, load next left block */
            while (!leftPQ.isEmpty()) {
                processRightRelation();

                if (outputBatch.isFull()) {
                    return outputBatch;
                }

                /** Handle cases where last is at its last right element */
                if (rightPQ.isEmpty() && hasLoadLastRightBatch && rightTuple != null) {
                    while (true) {
                        compareWithRightRelation();

                        if (outputBatch.isFull() || leftPQ.peek() == null) {
                            return outputBatch;
                        }

                        /** Need to check that left doesn't have any with similar value anymore */
                        if (rightTuple == null) {
                            /** Next left tuple is the same as current */
                            while (Tuple.compareTuples(leftPQ.peek(), leftTuple, leftAttrIndex) == 0) {
                                undoPQ();
                                leftTuple = leftPQ.poll();
                                if (rightPQ.isEmpty()) { // 1 tuple recovered from undoPQ, just compare with 1 other relation
                                    compareWithRightRelation();
                                } else { // many tuples recovered from undoPQ
                                    processRightRelation();
                                }

                                if (outputBatch.isFull() || leftPQ.peek() == null) {
                                    return outputBatch;
                                }
                            }
                            hasFinishRightRelation = true;
                            return outputBatch;
                        }
                    }
                } else if (rightPQ.isEmpty() && hasLoadLastRightBatch && rightTuple == null){ // exhausted last right element
                    hasFinishRightRelation = true;
                    return outputBatch;
                } else { // all other cases
                    return outputBatch;
                }
            }

            /** leftPQ is now empty, but yet to process last element of left */
            if (leftPQ.isEmpty() && hasLoadLastLeftBlock) {
                /** Case where left is last element, right is also last element */
                if (hasLoadLastRightBatch && rightPQ.isEmpty()) {
                    compareWithRightRelation();
                    hasFinishLeftRelation = true;
                    return outputBatch;
                    /** Cases where left is last element, but right have batches that have matching element */
                } else if (!hasLoadLastRightBatch) {
                    while (Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex) <= 0) {

                        processRightRelation();

                        /** Breaks when left becomes null, which is when right relation with same value has been exhausted */
                        if (leftTuple == null || outputBatch.isFull()) {
                            break;
                        }

                        /** Case where continously loading leads to end of right relation */
                        if (hasLoadLastRightBatch && rightPQ.isEmpty()) {
                            return outputBatch;
                        }
                    }
                    /** Cases where left is last element, right has some elements left */
                } else {
                    processRightRelation();
                    compareWithRightRelation(); // handle last tuple
                    if (outputBatch.isFull()) {
                        return outputBatch;
                    }
                }
                hasFinishLeftRelation = true;
                return outputBatch;
            }
        }

        return outputBatch;
    }


    /**
     * Process right relation, read right batch when rightPQ is empty
     */
    private void processRightRelation() {

        while (!rightPQ.isEmpty()) {
            if (leftTuple == null) {
                break;
            }
            compareWithRightRelation();

            if (outputBatch.isFull()) {
                return;
            }
        }
        readRightBatch();
    }

    /**
     * Compare left and right tuples, join tuples if they match the condition
     */
    private void compareWithRightRelation() {

        if (rightTuple == null || leftTuple == null) {
            return;
        }

        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);

        if (comparison == 0) { // matching join value, poll right
            Tuple outTuple = leftTuple.joinWith(rightTuple);
            outputBatch.add(outTuple);

        ///    tupleStack.push(rightTuple);
            rightTuple = rightPQ.poll();

            if (outputBatch.isFull()) { // after adding, check if is full
                return;
            }
        } else if (comparison > 0) { // left > right, progress right
            tupleStack.push(rightTuple);
            rightTuple = rightPQ.poll();
        } else { // left < right progress left
            if (leftPQ.peek() != null) {
                /** If next tuple is the same, right has progress more than left, restore */
                if (Tuple.compareTuples(leftPQ.peek(), leftTuple, leftAttrIndex) == 0) {
                    undoPQ();
                }
            }
            leftTuple = leftPQ.poll();
        }
    }

    private void undoPQ() {
        if (rightTuple != null) {
            tupleStack.push(rightTuple);
        }

        while (!tupleStack.isEmpty() && Tuple.compareTuples(leftTuple, tupleStack.peek(), leftAttrIndex, rightAttrIndex) <= 0) {
            rightPQ.add(tupleStack.pop());
        }

        rightTuple = rightPQ.poll();
    }

    public boolean close() {
        return (left.close() && right.close());
    }

//    private boolean leftIsGreaterThanRight(Tuple leftTuple, Tuple rightTuple) {
//
//        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);
//
//        if (comparison > 0) {
//            return true;
//        }
//        return false;
//    }
//
//    private boolean rightIsGreaterThanLeft(Tuple leftTuple, Tuple rightTuple) {
//
//        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);
//
//        if (comparison < 0) {
//            return true;
//        }
//
//        return false;
//    }
//
//    private boolean leftIsEqualToRight(Tuple leftTuple, Tuple rightTuple) {
//
//        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);
//
//        if (comparison == 0) {
//            return true;
//        }
//
//        return false;
//    }
//
//    private void combineTuplesIntoOutput(Tuple leftTuple, Tuple rightTuple, Batch outBatch) {
//        Tuple combine = leftTuple.joinWith(rightTuple);
//        outBatch.add(combine);
//    }
//
//    private boolean endOfLeftTable() {
//
//        try {
//            loadNextLeftTuple(leftTablePointer);
//            return false;
//        } catch (IndexOutOfBoundsException ie) {
//            return true;
//        }
//    }
//
//    private boolean endOfRightTable() {
//
//        try {
//            loadNextRightTuple(rightTablePointer);
//            return false;
//        } catch (IndexOutOfBoundsException ie) {
//            return true;
//        }
//    }
//
//    private Tuple loadNextLeftTuple(int index) {
//        int batchIndex, tupleIndexInBatch;
//
//        batchIndex = index / leftBatchSize;
//        tupleIndexInBatch = index % leftBatchSize;
//
//        readLeftBatch(batchIndex);
//        return batch.elementAt(tupleIndexInBatch);
//
//    }


    private void loadLeftBlock() {
        for (int i = 0; i < (numBuff - 2); i++) {
            try {
                Batch batch = (Batch) inputLeft.readObject();

                if (batch != null) {
                    for (int j = 0; j < batch.size(); j++) {
                        leftPQ.add(batch.elementAt(j));
                    }
                }
            } catch (EOFException e) {
                try { // 1 load all into buffer
                    if (isFirstBlock) {
                        leftTuple = leftPQ.poll();
                        isFirstBlock = false;
                    }

                    inputLeft.close();
                    hasLoadLastLeftBlock = true;
                    File f = new File(leftTableName + "0.0");
                    f.delete();
                } catch (IOException io) {
                    System.out.println("SortMergeJoin: Error in temp read");
                    System.exit(1);
                }
            } catch (ClassNotFoundException cnfe) {
                System.out.println("SortMergeJoin: Some error in deserialization.");
                System.exit(1);
            } catch (IOException io) {
                return;
            }
        }

        if (isFirstBlock) {
            leftTuple = leftPQ.poll();
            isFirstBlock = false;
        }
    }
//    private Tuple loadNextRightTuple(int index) {
//        int batchIndex, tupleIndexInBatch;
//
//        batchIndex = index / rightBatchSize;
//        tupleIndexInBatch = index % rightBatchSize;
//
//        readRightBatch(batchIndex);
//        return batch.elementAt(tupleIndexInBatch);
//    }

    private void readRightBatch() {


        try {
            Batch rightBatch = (Batch) inputRight.readObject();

            for (int i = 0; i < rightBatch.size(); i++) { // add tuples from right batch into right PQ
                rightPQ.add(rightBatch.elementAt(i));
            }

            if (isFirstBatch) {
                rightTuple = rightPQ.poll();
                isFirstBatch = false;
            }

        } catch (EOFException eof) { // Right relation has read the end
            try {
                inputRight.close();
                hasLoadLastRightBatch = true;
                File f = new File(rightTableName + "0.0");
                f.delete();
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in temp read");
                System.exit(1);
            }
        } catch (ClassNotFoundException cnfe) {
            System.out.println("SortMergeJoin:Error in deserializing");
            System.exit(1);
        } catch (IOException io) {
            return;
        }
    }

}
