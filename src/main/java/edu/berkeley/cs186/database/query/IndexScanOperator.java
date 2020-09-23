package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import edu.berkeley.cs186.database.table.stats.Histogram;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IndexScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;
  private String columnName;
  private QueryPlan.PredicateOperator predicate;
  private DataBox value;

  private int columnIndex;

  /**
   * An index scan operator.
   *
   * @param transaction the transaction containing this operator
   * @param tableName the table to iterate over
   * @param columnName the name of the column the index is on
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public IndexScanOperator(Database.Transaction transaction,
                           String tableName,
                           String columnName,
                           QueryPlan.PredicateOperator predicate,
                           DataBox value) throws QueryPlanException, DatabaseException {
    super(OperatorType.INDEXSCAN);
    this.tableName = tableName;
    this.transaction = transaction;
    this.columnName = columnName;
    this.predicate = predicate;
    this.value = value;
    this.setOutputSchema(this.computeSchema());
    columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
    this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);
 
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public String str() {
    return "type: " + this.getType() +
        "\ntable: " + this.tableName +
        "\ncolumn: " + this.columnName +
        "\noperator: " + this.predicate +
        "\nvalue: " + this.value;
  }

  /**
   * Estimates the table statistics for the result of executing this query operator.
   *
   * @return estimated TableStats
   */
  public TableStats estimateStats() throws QueryPlanException {
    TableStats stats;

    try {
      stats = this.transaction.getStats(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }

    return stats.copyWithPredicate(this.columnIndex,
                                   this.predicate,
                                   this.value);
  }

  /**
   * Estimates the IO cost of executing this query operator.
   * You should calculate this estimate cost with the formula
   * taught to you in class. Note that the index you've implemented
   * in this project is an unclustered index.
   *
   * You will find the following instance variables helpful:
   * this.transaction, this.tableName, this.columnName,
   * this.columnIndex, this.predicate, and this.value.
   *
   * You will find the following methods helpful: this.transaction.getStats,
   * this.transaction.getNumRecords, this.transaction.getNumIndexPages,
   * and tableStats.getReductionFactor.
   *
   * @return estimate IO cost
   * @throws QueryPlanException
   */
  public int estimateIOCost() throws QueryPlanException {
    /* TODO: Implement me! */
    return -1;
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new IndexScanIterator();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class IndexScanIterator implements Iterator<Record> {
    /* TODO: Implement the IndexScanIterator */
    private Iterator<Record> lookupItr;
    private Iterator<Record> sortedScanItr;
    private Iterator<Record> sortedFromItr;
    private Record nextRecord;
//    private String tableName;
    private int columnIndex;
    private DataBox value;
    private QueryPlan.PredicateOperator predicate;
    private Database.Transaction transaction;
    private boolean done;

    public IndexScanIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      if (! IndexScanOperator.this.transaction.indexExists(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName)) {
        throw new QueryPlanException("Index does not exist");
      }
      String tableName = IndexScanOperator.this.tableName;
      String columnName = IndexScanOperator.this.columnName;
      this.value = IndexScanOperator.this.value;
      this.columnIndex = IndexScanOperator.this.columnIndex;
      this.predicate = IndexScanOperator.this.predicate;
      this.transaction = IndexScanOperator.this.transaction;
      this.lookupItr = this.transaction.lookupKey(tableName, columnName, this.value);
      this.sortedScanItr = this.transaction.sortedScan(tableName, columnName);
      this.sortedFromItr = this.transaction.sortedScanFrom(tableName, columnName, this.value);
      this.done = false;
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
      switch (IndexScanOperator.this.predicate) {
        case EQUALS:
          if (lookupItr.hasNext()) {
            this.nextRecord = this.lookupItr.next();
            return true;
          }
          break;
        case LESS_THAN:
          if (! this.done && sortedScanItr.hasNext()) {
            Record currRecord = sortedScanItr.next();
            if (currRecord.getValues().get(this.columnIndex).compareTo(value) < 0) {
              this.nextRecord = currRecord;
              return true;
            }
            this.done = true;
          }
          break;
        case LESS_THAN_EQUALS:
          if (! this.done && sortedScanItr.hasNext()) {
            Record currRecord = sortedScanItr.next();
            if (currRecord.getValues().get(this.columnIndex).compareTo(value) < 0) {
              this.nextRecord = currRecord;
              return true;
            }
            this.done = true;
          }
          if (lookupItr.hasNext()) {
            this.nextRecord = this.lookupItr.next();
            return true;
          }
          break;
        case GREATER_THAN:
          while (this.lookupItr.hasNext()) {
            this.lookupItr.next();
            this.sortedFromItr.next();
          }
          if (this.sortedFromItr.hasNext()) {
            this.nextRecord = this.sortedFromItr.next();
            return true;
          }
          break;
        case GREATER_THAN_EQUALS:
          if (this.sortedFromItr.hasNext()) {
            this.nextRecord = this.sortedFromItr.next();
            return true;
          }
          break;
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
      if (hasNext()) {
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