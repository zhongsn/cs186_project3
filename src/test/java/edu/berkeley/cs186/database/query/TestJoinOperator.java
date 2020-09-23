package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class TestJoinOperator {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test(timeout=5000)
  public void testOperatorSchema() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    List<String> expectedSchemaNames = new ArrayList<String>();
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");

    List<DataBox> expectedSchemaTypes = new ArrayList<DataBox>();
    expectedSchemaTypes.add(new BoolDataBox());
    expectedSchemaTypes.add(new IntDataBox());
    expectedSchemaTypes.add(new StringDataBox(5));
    expectedSchemaTypes.add(new FloatDataBox());
    expectedSchemaTypes.add(new BoolDataBox());
    expectedSchemaTypes.add(new IntDataBox());
    expectedSchemaTypes.add(new StringDataBox(5));
    expectedSchemaTypes.add(new FloatDataBox());

    Schema expectedSchema = new Schema(expectedSchemaNames, expectedSchemaTypes);

    assertEquals(expectedSchema, joinOperator.getOutputSchema());
  }

  @Test(timeout=5000)
  public void testSimpleJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testEmptyJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator leftSourceOperator = new TestSourceOperator();

    List<Integer> values = new ArrayList<Integer>();
    TestSourceOperator rightSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();

    assertFalse(outputIterator.hasNext());
  }

  @Test(timeout=5000)
  public void testSimpleJoinPNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new PNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testSimpleJoinBNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testSimpleJoinGHJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new GraceHashOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }


  @Test(timeout=5000)
  public void testSimplePNLJOutputOrder() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath());
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();

    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 288; i++) {
      List<DataBox> vals;
      if (i < 144) {
        vals = r1Vals;
      } else {
        vals = r2Vals;
      }
      transaction.addRecord("leftTable", vals);
      transaction.addRecord("rightTable", vals);
    }

    for (int i = 0; i < 288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new PNLJOperator(s1, s2, "int", "int", transaction);

    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();

    while (outputIterator.hasNext()) {
      if (count < 20736) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*2) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*3) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*4) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*5) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*6) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*7) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else {
        assertEquals(expectedRecord1, outputIterator.next());
      }
      count++;
    }

    assertTrue(count == 165888);
  }

  @Test(timeout=5000)
  public void testSimpleSortMergeJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SortMergeOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }


  @Test(timeout=5000)
  public void testSimpleGHJOutputOrderUsingThreePartitions() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataBox> r3Vals = r3.getValues();
    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();

    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataBox val: r3Vals) {
        expectedRecordValues3.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 999; i++) {
      if (i % 3 == 0) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else if (i % 3 == 1) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r3Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new GraceHashOperator(s1, s2, "int", "int", transaction);
    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();

    while (outputIterator.hasNext()) {
      if (count < 333*333) {
        assertEquals(expectedRecord3, outputIterator.next());
      } else if (count < 333*333*2) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else {
        assertEquals(expectedRecord2, outputIterator.next());
      }
      count++;
    }
<<<<<<< HEAD

    @Test
    @Category(StudentTestP3.class)
    public void testEmptyPNLJ() throws QueryPlanException, DatabaseException, IOException {
        TestSourceOperator leftSourceOperator = new TestSourceOperator();

        List<Integer> values = new ArrayList<Integer>();
        TestSourceOperator rightSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
        File tempDir = tempFolder.newFolder("joinTest");
        Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
        JoinOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int", transaction);
        Iterator<Record> outputIterator = joinOperator.iterator();
        assertFalse(outputIterator.hasNext());
    }

    @Test
    @Category(StudentTestP3.class)
    public void testEmptyGraceHash() throws QueryPlanException, DatabaseException, IOException {
        List<Integer> values = new ArrayList<Integer>();
        TestSourceOperator leftSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

        TestSourceOperator rightSourceOperator = new TestSourceOperator();
        File tempDir = tempFolder.newFolder("joinTest");
        Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
        JoinOperator joinOperator = new GraceHashOperator(leftSourceOperator, rightSourceOperator, "int", "int", transaction);
        Iterator<Record> outputIterator = joinOperator.iterator();
        assertFalse(outputIterator.hasNext());
    }


    @Test
    @Category(StudentTestP3.class)
    public void testEmptySortMerge() throws QueryPlanException, DatabaseException, IOException {
        List<Integer> values = new ArrayList<Integer>();
        TestSourceOperator leftSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

        TestSourceOperator rightSourceOperator = new TestSourceOperator();
        File tempDir = tempFolder.newFolder("joinTest");
        Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
        JoinOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "int", "int", transaction);
        Iterator<Record> outputIterator = joinOperator.iterator();
        assertFalse(outputIterator.hasNext());
    }

    @Test
    @Category(StudentTestP3.class)
    public void testSimpleJoinGHJOnString() throws QueryPlanException, DatabaseException, IOException {
        TestSourceOperator sourceOperator = new TestSourceOperator();
        File tempDir = tempFolder.newFolder("joinTest");
        Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
        JoinOperator joinOperator = new GraceHashOperator(sourceOperator, sourceOperator, "string", "string", transaction);

        Iterator<Record> outputIterator = joinOperator.iterator();
        int numRecords = 0;

        List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
        expectedRecordValues.add(new BoolDataBox(true));
        expectedRecordValues.add(new IntDataBox(1));
        expectedRecordValues.add(new StringDataBox("abcde", 5));
        expectedRecordValues.add(new FloatDataBox(1.2f));
        expectedRecordValues.add(new BoolDataBox(true));
        expectedRecordValues.add(new IntDataBox(1));
        expectedRecordValues.add(new StringDataBox("abcde", 5));
        expectedRecordValues.add(new FloatDataBox(1.2f));
        Record expectedRecord = new Record(expectedRecordValues);

        while (outputIterator.hasNext()) {
            assertEquals(expectedRecord, outputIterator.next());
            numRecords++;
        }

        assertEquals(100*100, numRecords);
    }

    @Test
    @Category(StudentTestP3.class)
    public void testSortMergeJoinSortedOutputOrder() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();
        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();

        for (int i = 0; i < 2; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
        }

        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);
        d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
        d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

        for (int i = 0; i < 99; i++) {
            if (i % 3 == 0) {
                transaction.addRecord("leftTable", r3Vals);
                transaction.addRecord("rightTable", r1Vals);
            } else if (i % 3 == 1) {
                transaction.addRecord("leftTable", r2Vals);
                transaction.addRecord("rightTable", r3Vals);
            } else {
                transaction.addRecord("leftTable", r1Vals);
                transaction.addRecord("rightTable", r2Vals);
            }
        }

        QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
        QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
        QueryOperator joinOperator = new SortMergeOperator(s1, s2, "int", "int", transaction);
        int count = 0;
        Iterator<Record> outputIterator = joinOperator.iterator();

        while (outputIterator.hasNext()) {
            if (count < 33*33) {
                assertEquals(expectedRecord1, outputIterator.next());
            } else if (count < 33*33*2) {
                assertEquals(expectedRecord2, outputIterator.next());
            } else {
                assertEquals(expectedRecord3, outputIterator.next());
            }
            count++;
        }
        assertTrue(count == 33*33*3);
    }
=======
    assertTrue(count == 333*333*3);
  }


  @Test(timeout=5000)
  public void testBNLJDiffOutPutThanPNLJ() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataBox> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataBox> r4Vals = r4.getValues();
    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();

    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataBox val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataBox val: r4Vals) {
        expectedRecordValues4.add(val);
      }
    }
    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
    for (int i = 0; i < 2*288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r3Vals);
      } else if (i < 288) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r4Vals);
      } else if (i < 432) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r4Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }
    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new BNLJOperator(s1, s2, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    int count = 0;
    while (outputIterator.hasNext()) {
      Record r = outputIterator.next();
      if (count < 144 * 144) {
        assertEquals(expectedRecord3, r);
      } else if (count < 2 * 144 * 144) {
        assertEquals(expectedRecord4, r);
      } else if (count < 3 * 144 * 144) {
        assertEquals(expectedRecord1, r);
      } else {
        assertEquals(expectedRecord2, r);
      }
      count++;
    }
    assertTrue(count == 82944);

  }
>>>>>>> f7799f449ebbcd799aee2b9d8607aa334eb156c5

}