package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectorySubspace;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }

  // your code here
  private DirectorySubspace dir;
  private final Mode mode;
  public Mode getMode() {return mode;}
  private Transaction tx;
  private String tableName;
  private TableMetadata metadata;
  private int numAttributes;
  private final Database db;
  private KeySelector lastKey;
  private enum dirset {UNSET, FORWARD, BACKWARD}
  private dirset direction;
  private String queryAttrName;
  private Object queryAttrVal;
  private ComparisonOperator queryOp;
  private int lastRecordLength;
  private boolean lastDeleted = false;
  private boolean invalid = false;
  public boolean getInvalid() {return invalid;}
  private boolean eof = false;


  public Cursor(Database db) {
    mode = Mode.READ;
    dir = null;
    this.db = db;
    this.direction = dirset.UNSET;
  }
  public Cursor(Database db, String tableName, Mode mode) {
    this.mode = mode;
    this.db = db;
    this.tx = FDBHelper.openTransaction(db);
    this.tableName = tableName;
    this.dir = FDBHelper.openSubspace(tx, Collections.singletonList(tableName));
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    List<String> tblAttributePath = transformer.getTableAttributeStorePath();
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db,tx,tblAttributePath);
    metadata = transformer.convertBackToTableMetadata(kvPairs);
    FDBHelper.commitTransaction(tx);
    numAttributes = metadata.getAttributes().keySet().size();
    this.direction = dirset.UNSET;
    this.queryAttrVal = null;
    this.queryAttrName = null;
    this.queryOp = null;
  }
  public Cursor(Database db, String tableName, Mode mode, String attrName, Object attrVal, ComparisonOperator op) {
    this.mode = mode;
    this.db = db;
    this.tx = FDBHelper.openTransaction(db);
    this.tableName = tableName;
    this.dir = FDBHelper.openSubspace(tx,Collections.singletonList(tableName));
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    List<String> tblAttributePath = transformer.getTableAttributeStorePath();
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db,tx,tblAttributePath);
    metadata = transformer.convertBackToTableMetadata(kvPairs);
    FDBHelper.commitTransaction(tx);
    numAttributes = metadata.getAttributes().keySet().size();
    this.direction = dirset.UNSET;
    this.queryAttrName = attrName;
    this.queryAttrVal = attrVal;
    this.queryOp = op;
  }

  private boolean queryRecord(Record record) {
    Object val = record.getValueForGivenAttrName(this.queryAttrName);
    AttributeType type = record.getTypeForGivenAttrName(this.queryAttrName);
    if (val == null || this.queryAttrVal == null) return false;

    if (this.queryOp == ComparisonOperator.EQUAL_TO) {
      return val.equals(this.queryAttrVal);
    } else if (this.queryOp == ComparisonOperator.GREATER_THAN) {
      if (type == AttributeType.INT) {
        if (val instanceof Integer) {
          if (this.queryAttrVal instanceof Integer) {
            return (int) val > (int) this.queryAttrVal;
          }
          return (int) val > (long) this.queryAttrVal;
        }
        if (this.queryAttrVal instanceof Integer) {
          return (long) val > (int) this.queryAttrVal;
        }
        return (long) val > (long) this.queryAttrVal;
      } else if (type == AttributeType.DOUBLE) {
        return (double) val > (long) this.queryAttrVal;
      } else return false;
    } else if (this.queryOp == ComparisonOperator.LESS_THAN) {
      if (type == AttributeType.INT) {
        if (val instanceof Integer) {
          if (this.queryAttrVal instanceof Integer) {
            return (int) val < (int) this.queryAttrVal;
          }
          return (int) val < (long) this.queryAttrVal;
        }
        if (this.queryAttrVal instanceof Integer) {
          return (long) val < (int) this.queryAttrVal;
        }
        return (long) val < (long) this.queryAttrVal;
      } else if (type == AttributeType.DOUBLE) {
        return (double) val < (double) this.queryAttrVal;
      } else return false;
    } else if (this.queryOp == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      if (type == AttributeType.INT) {
        if (val instanceof Integer) {
          if (this.queryAttrVal instanceof Integer) {
            return (int) val >= (int) this.queryAttrVal;
          }
          return (int) val >= (long) this.queryAttrVal;
        }
        if (this.queryAttrVal instanceof Integer) {
          return (long) val >= (int) this.queryAttrVal;
        }
        return (long) val >= (long) this.queryAttrVal;
      } else if (type == AttributeType.DOUBLE) {
        return (double) val >= (long) this.queryAttrVal;
      } else return val.equals(this.queryAttrVal);
    } else if (this.queryOp == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) {
      if (type == AttributeType.INT) {
        if (val instanceof Integer) {
          if (this.queryAttrVal instanceof Integer) {
            return (int) val <= (int) this.queryAttrVal;
          }
          return (int) val <= (long) this.queryAttrVal;
        }
        if (this.queryAttrVal instanceof Integer) {
          return (long) val <= (int) this.queryAttrVal;
        }
        return (long) val <= (long) this.queryAttrVal;
      } else if (type == AttributeType.DOUBLE) {
        return (double) val <= (double) this.queryAttrVal;
      } else return val.equals(this.queryAttrVal);
    }
    return false;
  }

  public Record first() {
    if (this.direction == dirset.BACKWARD) return null;
    this.direction = dirset.FORWARD;
    tx = FDBHelper.openTransaction(db);

    KeySelector begin = KeySelector.firstGreaterOrEqual(dir.range().begin);
    KeySelector end = KeySelector.firstGreaterOrEqual(dir.range().end);

    List<KeyValue> kvs = tx.getRange(begin,end,numAttributes).asList().join();
    if (kvs.isEmpty()) return null;
    lastKey = KeySelector.firstGreaterOrEqual(kvs.get(0).getKey());
    Tuple pkTuple = Tuple.fromBytes(kvs.get(0).getKey());
    List<Object> pks = pkTuple.getNestedList(pkTuple.size()-2);

    Record record = new Record();
    lastRecordLength = 0;
    for (KeyValue kv : kvs){
      byte[] key = kv.getKey();

      Tuple keyTup = Tuple.fromBytes(key);
      if (!keyTup.getNestedList(keyTup.size()-2).equals(pks)) {
        break;
      }

      String attrName = keyTup.getString(keyTup.size()-1);
      byte[] val = kv.getValue();
      Tuple valTup = Tuple.fromBytes(val);
      Object attrVal = valTup.get(0);
      ++lastRecordLength;
      if (record.getValueForGivenAttrName(attrName) == null)
        record.setAttrNameAndValue(attrName,attrVal);

    }
    if (queryAttrName == null || queryRecord(record)) {
      return record;
    }
    return next();

  }

  public Record next() {
    if (this.direction != dirset.FORWARD) {
      return null;
    }

    KeySelector begin = KeySelector.firstGreaterOrEqual(lastKey.getKey());
    KeySelector end = KeySelector.firstGreaterOrEqual(dir.range().end);
    List<KeyValue> kvs = tx.getRange(begin,end,numAttributes*2).asList().join();

    if (kvs.isEmpty()) {
      eof = true;
      return null;
    }
    if (lastDeleted) {
      lastRecordLength = 0;
      lastDeleted = false;
    }
    kvs = kvs.subList(lastRecordLength,kvs.size());
    if (kvs.isEmpty()) {
      eof = true;
      return null;
    }
    lastKey = KeySelector.firstGreaterOrEqual(kvs.get(0).getKey());
    Tuple pkTuple = Tuple.fromBytes(kvs.get(0).getKey());
    List<Object> pks = pkTuple.getNestedList(pkTuple.size()-2);
//    System.out.print("next pks: ");
//    System.out.print(pks);


    lastRecordLength = 0;
    Record record = new Record();
    for (KeyValue kv : kvs) {
      byte[] key = kv.getKey();
      Tuple keyTup = Tuple.fromBytes(key);
//      System.out.println(keyTup.getNestedList(keyTup.size()-2));
      if (!keyTup.getNestedList(keyTup.size()-2).equals(pks)) {
        break;
      }
      String attrName = keyTup.getString(keyTup.size() - 1);
      byte[] val = kv.getValue();
      Tuple valTup = Tuple.fromBytes(val);
      Object attrVal = valTup.get(0);
      ++lastRecordLength;
      if (record.getValueForGivenAttrName(attrName) == null)
        record.setAttrNameAndValue(attrName, attrVal);
    }

    if (queryAttrName == null || queryRecord(record)) {
      return record;
    }
    return next();
  }

  public Record last() {
    if (this.direction == dirset.FORWARD) return null;
    this.direction = dirset.BACKWARD;

    tx = FDBHelper.openTransaction(db);

    KeySelector begin = KeySelector.firstGreaterOrEqual(dir.range().begin);
    KeySelector end = KeySelector.firstGreaterOrEqual(dir.range().end);
    List<KeyValue> kvs = tx.getRange(begin,end,numAttributes,true).asList().join();
    if (kvs.isEmpty()) return null;
    lastKey = KeySelector.firstGreaterOrEqual(kvs.get(0).getKey());
    Tuple pkTuple = Tuple.fromBytes(kvs.get(0).getKey());
    List<Object> pks = pkTuple.getNestedList(pkTuple.size()-2);

    lastRecordLength = 0;
    Record record = new Record();
    for (KeyValue kv : kvs){
      byte[] key = kv.getKey();

      Tuple keyTup = Tuple.fromBytes(key);
      if (!keyTup.getNestedList(keyTup.size()-2).equals(pks)) {
        break;
      }

      String attrName = keyTup.getString(keyTup.size()-1);
      byte[] val = kv.getValue();
      Tuple valTup = Tuple.fromBytes(val);
      Object attrVal = valTup.get(0);
      ++lastRecordLength;
      if (record.getValueForGivenAttrName(attrName) == null)
        record.setAttrNameAndValue(attrName,attrVal);

    }

    if (queryAttrName == null || queryRecord(record)) {
      return record;
    }

    return prev();
  }
  public Record prev() {
    if (this.direction != dirset.BACKWARD) {
      return null;
    }

    KeySelector begin = KeySelector.firstGreaterOrEqual(dir.range().begin);
    KeySelector end = KeySelector.firstGreaterThan(lastKey.getKey());
    List<KeyValue> kvs = tx.getRange(begin,end,numAttributes*2,true).asList().join();

    if (lastDeleted) {
      lastRecordLength = 0;
      lastDeleted = false;
    }
    kvs = kvs.subList(lastRecordLength, kvs.size());
    if (kvs.isEmpty()) {
      eof = true;
      return null;
    }
    lastKey = KeySelector.firstGreaterOrEqual(kvs.get(0).getKey());
    Tuple pkTuple = Tuple.fromBytes(kvs.get(0).getKey());
    List<Object> pks = pkTuple.getNestedList(pkTuple.size()-2);

    lastRecordLength = 0;
    Record record = new Record();
    for (KeyValue kv : kvs) {
      byte[] key = kv.getKey();
      Tuple keyTup = Tuple.fromBytes(key);
      if (!keyTup.getNestedList(keyTup.size()-2).equals(pks)) {
        break;
      }
      String attrName = keyTup.getString(keyTup.size() - 1);
      byte[] val = kv.getValue();
      Tuple valTup = Tuple.fromBytes(val);
      Object attrVal = valTup.get(0);
      ++lastRecordLength;
      if (record.getValueForGivenAttrName(attrName) == null)
        record.setAttrNameAndValue(attrName, attrVal);
    }

    if (queryAttrName == null || queryRecord(record)) {
      return record;
    }
    return prev();
  }

  public StatusCode update(String[] attrNames, Object[] attrVals) {
    if (mode == Mode.READ) {
      return StatusCode.CURSOR_INVALID;
    }
    if (!metadata.getAttributes().keySet().containsAll(List.of(attrNames))) {
      return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
    }
    if (direction == dirset.UNSET) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }
    if (eof) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    List<KeyValue> kvs;
    if (direction == dirset.FORWARD) {
      KeySelector begin = KeySelector.firstGreaterOrEqual(lastKey.getKey());
      KeySelector end = KeySelector.firstGreaterOrEqual(dir.range().end);
      kvs = tx.getRange(begin,end,numAttributes).asList().join();
    } else {
      KeySelector begin = KeySelector.firstGreaterOrEqual(dir.range().begin);
      KeySelector end = KeySelector.firstGreaterThan(lastKey.getKey());
      kvs = tx.getRange(begin,end,numAttributes,true).asList().join();
    }
    if (kvs == null || kvs.isEmpty()) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    for (int i = 0; i < attrNames.length; ++i) {
      for (KeyValue kv : kvs) {
        Tuple k = Tuple.fromBytes(kv.getKey());
        if (attrNames[i].equals(k.getString(k.size()-1))) {
          Tuple val = new Tuple().addObject(attrVals[i]);
          tx.set(kv.getKey(),val.pack());
          break;
        }
      }
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode commit() {
    tx.commit().join();
    tx.close();
    tx = FDBHelper.openTransaction(db);
    invalid = true;
    return StatusCode.SUCCESS;
  }
  public StatusCode abort() {
    FDBHelper.abortTransaction(tx);
    tx.close();
    tx = FDBHelper.openTransaction(db);
    return StatusCode.SUCCESS;
  }

  public StatusCode delete() {
    if (mode == Mode.READ) return StatusCode.CURSOR_INVALID;
    if (direction == dirset.UNSET) return StatusCode.CURSOR_NOT_INITIALIZED;
    if (eof) return StatusCode.CURSOR_REACH_TO_EOF;

    List<KeyValue> kvs;
    if (direction == dirset.FORWARD) {
      KeySelector begin = KeySelector.firstGreaterOrEqual(lastKey.getKey());
      KeySelector end = KeySelector.firstGreaterOrEqual(dir.range().end);
      kvs = tx.getRange(begin,end,numAttributes).asList().join();
    } else {
      KeySelector begin = KeySelector.firstGreaterOrEqual(dir.range().begin);
      KeySelector end = KeySelector.firstGreaterThan(lastKey.getKey());
      kvs = tx.getRange(begin,end,numAttributes,true).asList().join();
    }
    if (kvs == null || kvs.size() == 0) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }
    Tuple pkTuple = Tuple.fromBytes(kvs.get(0).getKey());
    List<Object> pks = pkTuple.getNestedList(pkTuple.size()-2);
//    System.out.print("del pks: ");
//    System.out.print(pks);
    lastRecordLength = 0;
    for (KeyValue kv : kvs) {
      Tuple ktup = Tuple.fromBytes(kv.getKey());
//      System.out.println(ktup.getNestedList(ktup.size()-2));
      if (!ktup.getNestedList(ktup.size()-2).equals(pks)) break;

      ++lastRecordLength;
      tx.clear(kv.getKey());
    }
    this.lastDeleted = true;
    return StatusCode.SUCCESS;
  }

}
