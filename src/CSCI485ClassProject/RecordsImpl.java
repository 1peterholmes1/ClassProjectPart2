package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class RecordsImpl implements Records {

  private Database db;

  public RecordsImpl() {
    db = FDBHelper.initialization();
  }

  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames,
      Object[] attrValues) {

    Transaction tx = FDBHelper.openTransaction(db);
    if(!FDBHelper.doesSubdirectoryExists(tx,Arrays.asList(tableName))){
      return StatusCode.TABLE_NOT_FOUND;
    }

    if (primaryKeys == null || primaryKeysValues == null || attrNames == null || attrValues == null) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }

    if (primaryKeys.length == 0 || primaryKeysValues.length == 0 || primaryKeys.length != primaryKeysValues.length) {
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }

    if (attrNames.length == 0 || attrValues.length == 0 || attrValues.length != attrNames.length) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }

    TableManager tm = new TableManagerImpl();
    HashMap<String, TableMetadata> metamap = tm.listTables();
    TableMetadata metadata = metamap.get(tableName);

    if (!metadata.getPrimaryKeys().containsAll(Arrays.asList(primaryKeys))) {
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }

    DirectorySubspace tabledir = FDBHelper.openSubspace(tx,Arrays.asList(tableName));
    List<FDBKVPair> pairs = new ArrayList<>();
    HashMap<String,AttributeType> existingAttrs = metadata.getAttributes();
    Record rec = new Record();
    for (int i = 0; i < primaryKeys.length; ++i) {
      AttributeType existingType = existingAttrs.get(primaryKeys[i]);
      if ((existingType == AttributeType.DOUBLE && !(primaryKeysValues[i] instanceof Double)) ||
              (existingType == AttributeType.INT && !(primaryKeysValues[i] instanceof Integer ||primaryKeysValues[i] instanceof Long)) ||
              (existingType == AttributeType.VARCHAR && !(primaryKeysValues[i] instanceof String))) {
        FDBHelper.abortTransaction(tx);
        return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
      }
      Tuple key = new Tuple().add(Arrays.asList(primaryKeysValues)).add(primaryKeys[i]);
      Tuple val = new Tuple().addObject(primaryKeysValues[i]);
      FDBKVPair fdbkvPair = new FDBKVPair(Collections.singletonList(tableName),key,val);

      if (tx.get(tabledir.pack(key)).join() != null) {
        tx.cancel();
        return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
      }
      FDBHelper.setFDBKVPair(tabledir,tx,fdbkvPair);
    }
    for (int i = 0; i < attrNames.length; ++i) {
      if (!metadata.doesAttributeExist(attrNames[i])) {
        AttributeType type = AttributeType.NULL;
        if (attrValues[i] instanceof Integer || attrValues[i] instanceof Long) {
          type = AttributeType.INT;
        } else if (attrValues[i] instanceof String) {
          type = AttributeType.VARCHAR;
        } else if (attrValues[i] instanceof Double) {
          type = AttributeType.DOUBLE;
        }
        tm.addAttribute(tableName,attrNames[i],type);
      } else {
        AttributeType existingType = existingAttrs.get(attrNames[i]);
        if ((existingType == AttributeType.DOUBLE && !(attrValues[i] instanceof Double)) ||
                (existingType == AttributeType.INT && !(attrValues[i] instanceof Integer || attrValues[i] instanceof Long)) ||
                (existingType == AttributeType.VARCHAR && !(attrValues[i] instanceof String))) {
          FDBHelper.abortTransaction(tx);
          return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
        }
      }

      Tuple key = new Tuple().add(Arrays.asList(primaryKeysValues)).add(attrNames[i]);
      Tuple val = new Tuple().addObject(attrValues[i]);
      FDBKVPair kvpair = new FDBKVPair(Collections.singletonList(tableName), key,val);
      FDBHelper.setFDBKVPair(tabledir,tx,kvpair);
    }

    tx.commit().join();

    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator,
      Cursor.Mode mode, boolean isUsingIndex) {
    return new Cursor(db,tableName,mode,attrName,attrValue,operator);
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    return new Cursor(db,tableName,mode);
  }

  @Override
  public Record getFirst(Cursor cursor) {
    return cursor.first();
  }

  @Override
  public Record getLast(Cursor cursor) {
    return cursor.last();
  }

  @Override
  public Record getNext(Cursor cursor) {
    return cursor.next();
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return cursor.prev();
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return cursor.update(attrNames,attrValues);
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    if (cursor.getInvalid()) return StatusCode.CURSOR_INVALID;
    return cursor.delete();
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    if (cursor.getMode() == Cursor.Mode.READ) return StatusCode.SUCCESS;
    return cursor.commit();
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    if (cursor.getMode() == Cursor.Mode.READ) return StatusCode.SUCCESS;
    return cursor.abort();
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    Cursor cursor = new Cursor(db,tableName, Cursor.Mode.READ_WRITE,attrNames[0],attrValues[0],ComparisonOperator.EQUAL_TO);
    Record record = cursor.first();
    while (record != null) {
      for (int i = 0; i < attrNames.length; ++i) {
        if (!record.getValueForGivenAttrName(attrNames[i]).equals(attrValues[i])) {
          cursor.next();
          continue;
        }
      }
      cursor.delete();
      cursor.commit();
      return StatusCode.SUCCESS;
    }
    return StatusCode.ATTRIBUTE_NOT_FOUND;
  }

}
