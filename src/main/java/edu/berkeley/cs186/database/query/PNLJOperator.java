package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
<<<<<<< HEAD
import edu.berkeley.cs186.database.table.RecordID;
=======
import edu.berkeley.cs186.database.table.stats.TableStats;
>>>>>>> f7799f449ebbcd799aee2b9d8607aa334eb156c5

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    /* TODO: Implement me! */
    return -1;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator implements Iterator<Record> {
    /* TODO: Implement the PNLJIterator */
    /* Suggested Fields */
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;
    private int leftNumEntries;
    private int rightNumEntries;

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      /* Suggested Starter Code: get table names. */
      if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (PNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      /* TODO */
      this.leftIterator = PNLJOperator.this.getPageIterator(this.leftTableName);
      if (this.leftIterator.hasNext()) {
        this.leftIterator.next();
      }
      this.leftNumEntries = PNLJOperator.this.getNumEntriesPerPage(this.leftTableName);
      this.rightNumEntries = PNLJOperator.this.getNumEntriesPerPage(this.rightTableName);
    }

    public boolean hasNext() {
      /* TODO */
      if (this.nextRecord != null) {
        return true;
      }
      while (true) {
        if (leftPage == null) {
          if (this.leftIterator.hasNext()) {
            this.leftPage = this.leftIterator.next();
            try {
              this.leftHeader = PNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
              this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
              if (this.rightIterator.hasNext()) {
                this.rightIterator.next();
              }
            } catch (DatabaseException e) {
              return false;
            }
          } else {
            return false;
          }
        }

        while (this.rightRecord != null || this.rightIterator.hasNext()) {
          if (this.rightRecord == null) {
            this.rightPage = this.rightIterator.next();
            try {
              this.rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
            } catch (DatabaseException e) {
              return false;
            }
            this.leftEntryNum = 0;
            this.leftRecord = getNextLeftRecordInPage();
          }
          while (this.leftRecord != null) {
            if (this.rightRecord == null) {
              this.rightEntryNum = 0;
            }
            this.rightRecord = getNextRightRecordInPage();
            while (this.rightRecord != null) {
              DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
              DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
              if (leftJoinValue.equals(rightJoinValue)) {
                List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
                List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
                leftValues.addAll(rightValues);
                this.nextRecord = new Record(leftValues);
                return true;
              }
              this.rightRecord = getNextRightRecordInPage();
            }
            this.leftRecord = getNextLeftRecordInPage();
          }
        }
        this.leftPage = null;
      }
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

    private Record getNextLeftRecordInPage() {
      /* TODO */
      while (this.leftEntryNum < leftNumEntries) {
        Record r = getRecord(PNLJOperator.this.getLeftSource(), this.leftPage, this.leftHeader,
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
        Record r = getRecord(PNLJOperator.this.getRightSource(), this.rightPage, this.rightHeader,
                this.rightEntryNum);
        this.rightEntryNum++;
        if (r != null) {
          return r;
        }
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
  }
}