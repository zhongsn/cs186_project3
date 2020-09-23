package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    /* TODO: Implement me! */
    return -1;
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    /* TODO: Implement the BNLJIterator */
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
    private List<Page> leftBlock;
    private int leftPageIndex;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      this.leftIterator = BNLJOperator.this.getPageIterator(this.leftTableName);
      if (this.leftIterator.hasNext()) {
        this.leftIterator.next();
      }
      this.leftNumEntries = BNLJOperator.this.getNumEntriesPerPage(this.leftTableName);
      this.rightNumEntries = BNLJOperator.this.getNumEntriesPerPage(this.rightTableName);
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
      while (true) {
        if (this.leftBlock == null) {
          this.leftBlock = new ArrayList<Page>();
          for (int i = 0; i < BNLJOperator.this.numBuffers - 2 && this.leftIterator.hasNext(); i++) {
            this.leftBlock.add(this.leftIterator.next());
          }
          if (this.leftBlock.isEmpty()){
            return false;
          }
          try {
            this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
            if (this.rightIterator.hasNext()) {
              this.rightIterator.next();
            }
          } catch (DatabaseException e) {
            return false;
          }
        }

        while (this.rightRecord != null || this.rightIterator.hasNext()) {
          if (this.rightRecord == null) {
            this.rightPage = this.rightIterator.next();
            try {
              this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
            } catch (DatabaseException e) {
              return false;
            }
            this.leftPageIndex = 0;
            this.leftPage = this.leftBlock.get(0);
            try {
              this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
            } catch (DatabaseException e) {
              return false;
            }
            this.leftEntryNum = 0;
            this.leftRecord = getNextLeftRecordInBlock();
          }
          while (this.leftRecord != null) {
            if (this.rightRecord == null) {
              this.rightEntryNum = 0;
            }
            this.rightRecord = getNextRightRecordInPage();
            while (this.rightRecord != null) {
              DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
              DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
              if (leftJoinValue.equals(rightJoinValue)) {
                List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
                List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
                leftValues.addAll(rightValues);
                this.nextRecord = new Record(leftValues);
                return true;
              }
              this.rightRecord = getNextRightRecordInPage();
            }
            this.leftRecord = getNextLeftRecordInBlock();
          }
        }
        this.leftBlock = null;
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

    private Record getNextLeftRecordInBlock() {
      /* TODO */
      while (true) {
        while (this.leftEntryNum < leftNumEntries) {
          Record r = getRecord(BNLJOperator.this.getLeftSource(), this.leftPage, this.leftHeader,
                  this.leftEntryNum);
          this.leftEntryNum++;
          if (r != null) {
            return r;
          }
        }
        this.leftPageIndex++;
        if (this.leftPageIndex >= this.leftBlock.size()) {
          break;
        }
        this.leftEntryNum = 0;
        this.leftPage = this.leftBlock.get(this.leftPageIndex);
        try {
          this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
        } catch (DatabaseException e) {
          return null;
        }
      }
      return null;
    }


    private Record getNextRightRecordInPage() {
      /* TODO */
      while (this.rightEntryNum < rightNumEntries) {
        Record r = getRecord(BNLJOperator.this.getRightSource(), this.rightPage, this.rightHeader,
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