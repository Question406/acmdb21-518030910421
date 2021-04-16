package simpledb;

import jdk.jfr.Timestamp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc td = null;
    private RecordId rid = null;
    private Field[] filedAr = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        this.filedAr = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        this.filedAr[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return this.filedAr[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        List<String> resList = Arrays.stream(this.filedAr).map(Field::toString).collect(Collectors.toList());
        return String.join("\t", resList);
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return Arrays.stream(filedAr).iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.td = td;
    }

    /**
     * Merge two tuples into a new tuple
     * @param t1 the tuple to merge
     * @param t2 the tuple to merge
     * @return the new merged tuple
     */
    public static Tuple merge(Tuple t1, Tuple t2) {
        TupleDesc mergedTupDesc = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple resTup = new Tuple(mergedTupDesc);
        int i = 0, j = 0;
        for (; i < t1.getTupleDesc().numFields(); i++) {
            resTup.setField(i, t1.getField(i));
        }
        for (; j < t2.getTupleDesc().numFields(); j++) {
            resTup.setField(i + j, t2.getField(j));
        }
        return resTup;
    }
}
