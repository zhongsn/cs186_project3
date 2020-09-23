package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import org.omg.CORBA.INTERNAL;

import java.lang.reflect.Array;
import java.util.*;
import java.lang.*;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }
  
  public int estimateIOCost() throws QueryPlanException {
    // You don't need to implement this.
    throw new QueryPlanException("Not yet implemented!");
  }

  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator implements Iterator<Record> {
    /* TODO: Implement the SortMergeIterator */
    private String leftTableName;
    private String rightTableName;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private Record recordMark;
    private Page leftPage;
    private Page rightPage;
    private Iterator<Page> leftPageItr;
    private Iterator<Page> rightPageItr;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;
    private int leftNumEntries;
    private int rightNumEntries;
    private int entryNumMark;
    private DataBox joinValueMark;
    private DataBox leftJoinValue;
    private DataBox rightJoinValue;
    private boolean yielded;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      this.leftTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getLeftColumnName() + "Left";
      this.rightTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getRightColumnName() + "Right";
      SortMergeOperator.this.createTempTable(getLeftSource().getOutputSchema(), this.leftTableName);
      SortMergeOperator.this.createTempTable(getRightSource().getOutputSchema(), this.rightTableName);

      Iterator<Record> leftIterator = getLeftSource().iterator();
      Iterator<Record> rightIterator = getRightSource().iterator();
      List<Record> leftRecords = new ArrayList<Record>();
      List<Record> rightRecords = new ArrayList<Record>();
      while (leftIterator.hasNext()) {
        leftRecords.add(leftIterator.next());
      }
      while (rightIterator.hasNext()) {
        rightRecords.add(rightIterator.next());
      }
      Collections.sort(leftRecords, new LeftRecordComparator());
      Collections.sort(rightRecords, new RightRecordComparator());

      leftIterator = leftRecords.iterator();
      rightIterator = rightRecords.iterator();
      while (leftIterator.hasNext()) {
        SortMergeOperator.this.addRecord(this.leftTableName, leftIterator.next().getValues());
      }
      while (rightIterator.hasNext()) {
        SortMergeOperator.this.addRecord(this.rightTableName, rightIterator.next().getValues());
      }

      this.leftPageItr = SortMergeOperator.this.getPageIterator(this.leftTableName);
      this.rightPageItr = SortMergeOperator.this.getPageIterator(this.rightTableName);
      if (this.leftPageItr.hasNext()) {
        this.leftPageItr.next();
      }
      if (this.rightPageItr.hasNext()) {
        this.rightPageItr.next();
      }
      this.leftNumEntries = SortMergeOperator.this.getNumEntriesPerPage(this.leftTableName);
      this.rightNumEntries = SortMergeOperator.this.getNumEntriesPerPage(this.rightTableName);
      this.yielded = false;
    }


    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      if (this.nextRecord != null) {
        return true;
      }
      if (this.leftPage == null && this.rightPage == null) {
        if (!this.leftPageItr.hasNext() || !this.rightPageItr.hasNext()) {
          return false;
        }
        this.leftPage = this.leftPageItr.next();
        this.rightPage = this.rightPageItr.next();
        this.leftEntryNum = 0;
        this.rightEntryNum = 0;
        try {
          this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTableName, this.leftPage);
          this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTableName, this.rightPage);
        } catch (DatabaseException e) {
          return false;
        }
        if (! advanceLeftTable() || ! advanceRightTable()) {
          return false;
        }
      }
      while (true) {
        if (! yielded) {
          while (leftJoinValue.compareTo(rightJoinValue) < 0) {
            if (!advanceLeftTable()) {
              return false;
            }
          }
          while (leftJoinValue.compareTo(rightJoinValue) > 0) {
            if (!advanceRightTable()) {
              return false;
            }
          }
          this.entryNumMark = this.rightEntryNum;
          this.joinValueMark = this.rightJoinValue;
          this.recordMark = this.rightRecord;
        }
        while (yielded || this.leftJoinValue.equals(this.rightJoinValue)) {
          if (yielded) {
            yielded = false;
          }
          while (this.rightJoinValue != null && this.leftJoinValue.equals(this.rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            advanceRightTable();
            yielded = true;
            return true;
          }
          this.rightEntryNum = this.entryNumMark;
          this.rightJoinValue = this.joinValueMark;
          this.rightRecord = this.recordMark;
          if (! advanceLeftTable()) {
            return false;
          }
        }
      }
    }

    private boolean advanceLeftTable() {
      /* TODO */
      this.leftRecord = getNextLeftRecordInPage();
      while (this.leftRecord == null) {
        if (! this.leftPageItr.hasNext()) {
          this.leftJoinValue = null;
          return false;
        }
        this.leftPage = this.leftPageItr.next();
        this.leftEntryNum = 0;
        try {
          this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTableName, this.leftPage);
        } catch (DatabaseException e) {
          return false;
        }
        this.leftRecord = getNextLeftRecordInPage();
      }
      this.leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
      return true;
    }

    private boolean advanceRightTable() {
      /* TODO */
      this.rightRecord = getNextRightRecordInPage();
      while (this.rightRecord == null) {
        if (! this.rightPageItr.hasNext()) {
          this.rightJoinValue = null;
          return false;
        }
        this.rightPage = this.rightPageItr.next();
        this.rightEntryNum = 0;
        try {
          this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTableName, this.rightPage);
        } catch (DatabaseException e) {
          return false;
        }
        this.rightRecord = getNextRightRecordInPage();
      }
      this.rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
      return true;
    }

    private Record getNextLeftRecordInPage() {
      /* TODO */
      while (this.leftEntryNum < leftNumEntries) {
        Record r = getRecord(SortMergeOperator.this.getLeftSource(), this.leftPage, this.leftHeader,
                this.leftEntryNum);
        this.leftEntryNum++;
        if (r != null) {
          return r;
        }
      }
      return null;
    }

    private Record getNextRightRecordInPage() {
      /* TODO */
      while (this.rightEntryNum < rightNumEntries) {
        Record r = getRecord(SortMergeOperator.this.getRightSource(), this.rightPage, this.rightHeader,
                this.rightEntryNum);
        this.rightEntryNum++;
        if (r != null) {
          return r;
        }
      }
      return null;
    }

    private Record getRecord(QueryOperator source, Page p, byte[] header, int entryNum) {
      byte b = header[entryNum/8];
      int bitOffset = 7 - (entryNum % 8);
      byte mask = (byte) (1 << bitOffset);
      byte value = (byte) (b & mask);
      if (value != 0) {
        int entrySize = source.getOutputSchema().getEntrySize();
        int offset = header.length + (entrySize * entryNum);
        byte[] bytes = p.readBytes(offset, entrySize);
        return source.getOutputSchema().decode(bytes);
      }
      return null;
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
        Record r = this.nextRecord;
        this.nextRecord = null;
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}