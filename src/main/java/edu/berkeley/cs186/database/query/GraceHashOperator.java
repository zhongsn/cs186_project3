package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    /* TODO: Implement me! */
    return -1;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private String[] leftPartitions;
    private String[] rightPartitions;

    private HashMap<Integer, List<Record>>[] leftHashMaps;
    private Iterator<Record> rightPartitionItr;
    private Record rightRecord;
    private int currPartitionIndex;
    private Iterator<Record> nextLeftRecordsItr;
    /* TODO: Implement the GraceHashOperator */

    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }
      /* TODO */
      Record leftRecord;
      while (this.leftIterator.hasNext()) {
        leftRecord = this.leftIterator.next();
        DataBox leftJoinValue = leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
        int partitionIndex = leftJoinValue.hashCode() % (numBuffers -1);
        GraceHashOperator.this.addRecord(leftPartitions[partitionIndex], leftRecord.getValues());
      }
      Record rightRecord;
      while (this.rightIterator.hasNext()) {
        rightRecord = this.rightIterator.next();
        DataBox rightJoinValue = rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
        int partitionIndex = rightJoinValue.hashCode() % (numBuffers -1);
        GraceHashOperator.this.addRecord(rightPartitions[partitionIndex], rightRecord.getValues());
      }

      this.leftHashMaps = new HashMap[numBuffers - 1];
      for (int i = 0; i < numBuffers - 1; i++) {
        HashMap<Integer, List<Record>> leftHashMap = new HashMap<Integer, List<Record>>();
        Iterator<Record> tableItr = GraceHashOperator.this.getTableIterator(this.leftPartitions[i]);
        while (tableItr.hasNext()) {
          leftRecord = tableItr.next();
          DataBox leftJoinValue = leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
          int key = leftJoinValue.hashCode() % (numBuffers -1);
          if (leftHashMap.containsKey(key)) {
            leftHashMap.get(key).add(leftRecord);
          } else {
            List<Record> newList = new ArrayList<Record>();
            newList.add(leftRecord);
            leftHashMap.put(key, newList);
          }
        }
        this.leftHashMaps[i] = leftHashMap;
      }

      this.currPartitionIndex = 0;

    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      if (this.nextLeftRecordsItr != null && this.nextLeftRecordsItr.hasNext()) {
        return true;
      }
      while (currPartitionIndex < numBuffers - 1) {
        if (rightPartitionItr == null) {
          try {
            this.rightPartitionItr = GraceHashOperator.this.getTableIterator(this.rightPartitions[this.currPartitionIndex]);
          } catch (DatabaseException e) {
            return false;
          }
        }
        HashMap<Integer, List<Record>> currHashMap = this.leftHashMaps[this.currPartitionIndex];
        while ( this.rightPartitionItr.hasNext()) {
          Record rightRecord = this.rightPartitionItr.next();
          DataBox rightJoinValue = rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
          int key = rightJoinValue.hashCode() % (numBuffers -1);
          if (currHashMap.containsKey(key)) {
            this.rightRecord = rightRecord;
            this.nextLeftRecordsItr = currHashMap.get(key).iterator();
            return true;
          }
        }
        currPartitionIndex++;
        this.rightPartitionItr = null;
      }
      return false;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      /* TODO */
      if (this.hasNext()) {
        Record leftRecord = this.nextLeftRecordsItr.next();
        List<DataBox> leftValues = new ArrayList<DataBox>(leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
        leftValues.addAll(rightValues);
        Record r = new Record(leftValues);
        return r;
      } else {
        throw new NoSuchElementException();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}