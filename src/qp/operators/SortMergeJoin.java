package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 *  1st Step -> Sort both left and right tables
 *  2nd Step -> Scan left table once. Each right table partition is scanned once per matching left table tuple
 *  3rd Step -> Do a merge and output tuples
 */
public class SortMergeJoin extends Join {


    private int batchSize;

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

    private List<File> leftFiles;
    private List<File> rightFiles;

    private Batch leftBuffer;
    private int leftBufferIndex = -1;

    private int rightBufferOffset = 0;
    private List<Batch> rightBuffer = new LinkedList<>();
    private Batch rightRunningBuffer;
    private int rightRunningBufferIndex = -1;
    private int rightBufferSize;

    private PriorityQueue<Tuple> leftPQ;
    private PriorityQueue<Tuple> rightPQ;

    private boolean hasLoadLastLeftBlock;
    private boolean hasLoadLastRightBatch;

    private boolean hasFinishLeftRelation;
    private boolean hasFinishRightRelation;

    private boolean isFirstBatch = true;
    private boolean isFirstBlock = true;

    private static boolean CLEANUP_FILES = true;


    public SortMergeJoin(Join jn){
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
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

//        leftFiles = writeOperatorToFile(leftTable, "Left_SMJ");
//        rightFiles = writeOperatorToFile(rightTable, "Right_SMJ");

        rightBufferSize = numBuff - 3;



//        leftTable.close();
//        rightTable.close();

        try {
            inputLeft = new ObjectInputStream(new FileInputStream(leftTableName + "0.0"));
            inputRight = new ObjectInputStream(new FileInputStream(rightTableName + "0.0"));
        } catch (IOException io) {
            System.err.println("SortMergeJoin:Error in reading in input files");
            System.exit(1);
        }

        leftPQ = new PriorityQueue<>((t1, t2) -> Tuple.compareTuples(t1, t2, leftAttrIndex));
        rightPQ = new PriorityQueue<>((t1, t2) -> Tuple.compareTuples(t1, t2, rightAttrIndex));

//        try {
//            initializeRightBuffer();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (NullPointerException n) {
//            n.printStackTrace();
//        }

        return true;
    }

    /**
     * When left table tuples matches with right table tuples, output the result pair to a batch
     * @return a batch of tuple result pair
     */
    public Batch next() {

        Batch outBatch = new Batch(batchSize);

        Tuple rightTuple = loadNextRightTuple(rightTablePointer);
        Tuple leftTuple = loadNextLeftTuple(leftTablePointer);

        while (!(outBatch.isFull() || endOfLeftTable() || endOfRightTable()) ) {


            if (leftIsGreaterThanRight(leftTuple,rightTuple)) {
                rightTablePointer++;
                loadNextRightTuple(rightTablePointer);
            }
            else if (rightIsGreaterThanLeft(leftTuple,rightTuple)) {
                leftTablePointer++;
                loadNextLeftTuple(leftTablePointer);
            }
            else {

                combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
                rightTablePointer++;
                loadNextRightTuple(rightTablePointer);

                while(!endOfRightTable() && leftIsEqualToRight(leftTuple, rightTuple)) {

                    combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
                    rightTablePointer++;
                    loadNextRightTuple(rightTablePointer);
                }

                leftTablePointer++;
                loadNextLeftTuple(leftTablePointer);
                while(!endOfLeftTable() && leftIsEqualToRight(leftTuple, rightTuple)) {

                    combineTuplesIntoOutput(leftTuple, rightTuple, outBatch);
                    leftTablePointer++;
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
        rightBuffer.clear();
        leftBuffer.clear();

        if (CLEANUP_FILES) {
            for (File file: leftFiles) {
                file.delete();
            }

            for (File file: rightFiles) {
                file.delete();
            }
        }

        return super.close();
    }

    private boolean leftIsGreaterThanRight(Tuple leftTuple, Tuple rightTuple) {

        int comparison = Tuple.compareTuples(leftTuple,rightTuple,leftAttrIndex,rightAttrIndex);

        if (comparison > 0) {
            return true;
        }
        return false;
    }

    private boolean rightIsGreaterThanLeft(Tuple leftTuple, Tuple rightTuple) {

        int comparison = Tuple.compareTuples(leftTuple,rightTuple, leftAttrIndex, rightAttrIndex);

        if (comparison < 0) {
            return true;
        }

        return false;
    }

    private boolean leftIsEqualToRight(Tuple leftTuple, Tuple rightTuple) {

        int comparison = Tuple.compareTuples(leftTuple,rightTuple,leftAttrIndex,rightAttrIndex);

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

        Batch batch = readLeftBatch(batchIndex);
        return batch.elementAt(tupleIndexInBatch);
    }

    private Batch readLeftBatch(int batchIndex) {

//        if (batchIndex == leftBufferIndex) {
//            return leftBuffer;
//        }
//        File file = leftFiles.get(batchIndex);
//        try {
//            leftBuffer = readBatchFromFile(file);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        leftBufferIndex = batchIndex;
//        return leftBuffer;
        Batch leftBatchOutput = new Batch(leftBatchSize);
        Batch output = new Batch(leftBatchSize);

        int currentBatch = 0;

        while (leftBatchOutput != null) {
            try {
                leftBatchOutput = (Batch) inputLeft.readObject();
            } catch (IOException io) {
                System.err.println("SortMergeJoin:read left batch error");
                System.exit(1);
            } catch (ClassNotFoundException ce) {
                System.err.println("SortMergeJoin:read left batch error");
                System.exit(1);
            }

            if (leftBatchOutput != null && batchIndex == currentBatch) {
                output = leftBatchOutput;
            }
            currentBatch++;
        }

        return output;
    }

    private Tuple loadNextRightTuple(int index) {
        int batchIndex, tupleIndexInBatch;

        batchIndex = index / rightBatchSize;
        tupleIndexInBatch = index % rightBatchSize;

        Batch batch = readRightBatch(batchIndex);
        return batch.elementAt(tupleIndexInBatch);
    }

    private void initializeRightBuffer() throws IOException, ClassNotFoundException {
        rightBufferOffset = 0;
        rightBuffer.clear();

        for (int i = 0;i < rightBufferSize;i++) {
            if (i + 1 >= rightFiles.size()) {
                break;
            }
            File file = rightFiles.get(i);
            Batch batch = readBatchFromFile(file);
            rightBuffer.add(batch);
        }
    }

    private Batch readRightBatch(int batchIndex) {
//        if (isInRightBuffer(batchIndex)) {
//            return readFromBuffer(batchIndex);
//        }
//        if (batchIndex < rightBufferOffset || rightBufferSize == 0) {
//            try {
//                return readToRunningBuffer(batchIndex);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ClassNotFoundException e) {
//                e.printStackTrace();
//            }
//        }
//        while (!isInRightBuffer(batchIndex)) {
//            try {
//                advanceBuffer();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ClassNotFoundException e) {
//                e.printStackTrace();
//            }
//        }
//        return readFromBuffer(batchIndex);

        Batch rightBatchOutput = new Batch(rightBatchSize);
        Batch output = new Batch(rightBatchSize);

        int currentBatch = 0;

        while (rightBatchOutput != null) {
            try {
                rightBatchOutput = (Batch) inputRight.readObject();
            } catch (EOFException eof) { // Right relation has read the end
                try {
                    inputRight.close();
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
                System.err.println("SortMergeJoin:IO error in reading right batch" + io);

                if (rightBatchOutput != null && batchIndex == currentBatch) {
                    output = rightBatchOutput;
                }
                break;
            }

            if (rightBatchOutput != null && batchIndex == currentBatch) {
                output = rightBatchOutput;
            }
            currentBatch++;
        }

        return output;
    }

    private void advanceBuffer() throws IOException, ClassNotFoundException {
        int nextRightBatchToRead = rightBufferOffset + rightBufferSize;
        rightBuffer.remove(0);
        Batch batch = readBatchFromFile(rightFiles.get(nextRightBatchToRead));
        rightBuffer.add(batch);
        rightBufferOffset++;
    }

    private Batch readToRunningBuffer(int index) throws IOException, ClassNotFoundException {
        if (rightRunningBufferIndex == index) {
            return rightRunningBuffer;
        }

        File file = rightFiles.get(index);
        rightRunningBuffer = readBatchFromFile(file);
        rightRunningBufferIndex = index;
        return rightRunningBuffer;
    }

    private Batch readFromBuffer(int index) {
        return rightBuffer.get(index - rightBufferOffset);
    }

    private boolean isInRightBuffer(int index) {
        return (rightBufferOffset <= index) && (index < rightBufferOffset + rightBufferSize);
    }


    private Batch readBatchFromFile(File file) throws IOException, ClassNotFoundException {
        ObjectInputStream in = null;
        try {
            in = new ObjectInputStream(new FileInputStream(file));
        } catch (IOException io) {
            System.err.println("SortMergeJoin:Error reading batch from file");
            System.exit(1);
        }
        return (Batch) in.readObject();

    }

    private void writeBatchToFile(Batch batch, File file) {
        ObjectOutputStream out;

        try {
            out = new ObjectOutputStream(new FileOutputStream(file));
        } catch (IOException io) {
            System.err.println("SortMergeJoin:Error reading batch from file");
            System.exit(1);
        }
    }

    private List<File> writeOperatorToFile(Operator operator, String prefix) {
        Batch batch;
        int count = 0;
        List<File> files = new ArrayList<>();

        while ((batch = operator.next()) != null) {
            File file = new File(prefix + "-" + count);
            count++;
            writeBatchToFile(batch,file);
            files.add(file);
        }
        return files;
    }
}
