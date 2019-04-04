package qp.utils;

import java.util.Vector;
import java.io.Serializable;

/**
 * This class represents a block, to be used for Block Nested Loops Join (BNLJ).
 * A block contains a number of pages.
 */

public class Block implements Serializable {
    int MAX_SIZE;  // Number of tuples per page
    int pageSize;  /* Number of bytes per page**/

    Vector tuples; // The tuples in the page
    Vector batches; // The pages in the block

    public Block(int numPages, int pageSize) {
        MAX_SIZE = numPages;
        this.pageSize = pageSize;
        tuples = new Vector<>(MAX_SIZE * numPages);
        batches = new Vector(MAX_SIZE);
    }

    /**
     * Adds a new Batch to this Block
     * @param batch the batch to be added
     */
    public void addBatch(Batch batch) {
        if (batches.size() < MAX_SIZE) {
            batches.add(batch);
            for (int i = 0; i < batch.size(); i++) {
                tuples.add(batch.elementAt(i));
            }
        }
    }

    public int capacity() {
        return MAX_SIZE;
    }

    public int size() {
        return tuples.size();
    }

    public Tuple elementAt(int i) {
        return (Tuple) tuples.elementAt(i);
    }

    public boolean isFull() {
        return size() == capacity();
    }

    public boolean isEmpty() {
        return batches.isEmpty();
    }
}
