package CSCI485ClassProject;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import CSCI485ClassProject.models.Record;

import java.util.ArrayList;
import java.util.List;

public class FDBHelper {

  public static int FDB_API_VERSION = DBConf.FDB_API_VERSION;

  public static int MAX_TRANSACTION_COMMIT_RETRY_TIMES = 20;

  public static Database initialization() {
    FDB fdb = FDB.selectAPIVersion(FDB_API_VERSION);

    Database db = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    return db;
  }

  public static void setFDBKVPair(DirectorySubspace tgtSubspace, Transaction tx, FDBKVPair kv) {
    if (tgtSubspace == null) {
      tgtSubspace = FDBHelper.createOrOpenSubspace(tx, kv.getSubspacePath());
    }
    tx.set(tgtSubspace.pack(kv.getKey()), kv.getValue().pack());
  }

  public static List<String> getAllDirectSubspaceName(Transaction tx) {
    List<String> subpaths = DirectoryLayer.getDefault().list(tx).join();
    return subpaths;
  }

  public static List<FDBKVPair> getAllKeyValuePairsOfSubdirectory(Database db, Transaction tx, List<String> path) {
    List<FDBKVPair> res = new ArrayList<>();
    if (!doesSubdirectoryExists(tx, path)) {
      return res;
    }

    DirectorySubspace dir = FDBHelper.createOrOpenSubspace(tx, path);
    Range range = dir.range();

    List<KeyValue> kvs = tx.getRange(range).asList().join();
    for (KeyValue kv : kvs) {
      Tuple key = dir.unpack(kv.getKey());
      Tuple value = Tuple.fromBytes(kv.getValue());
      res.add(new FDBKVPair(path, key, value));
    }

    return res;
  }

  public static FDBKVPair getCertainKeyValuePairInSubdirectory(DirectorySubspace dir, Transaction tx, Tuple keyTuple, List<String> path) {
    if (dir == null) {
      return null;
    }
    byte[] valBytes = tx.get(dir.pack(keyTuple)).join();
    if (valBytes == null) {
      return null;
    }
    Tuple value = Tuple.fromBytes(valBytes);
    return new FDBKVPair(path, keyTuple, value);
  }

  public static void clear(Database db) {
    Transaction tx = openTransaction(db);
    final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
    final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
    tx.clear(st, en);
    commitTransaction(tx);
  }

  public static DirectorySubspace createOrOpenSubspace(Transaction tx, List<String> path) {
    return DirectoryLayer.getDefault().createOrOpen(tx, path).join();
  }

  public static DirectorySubspace openSubspace(Transaction tx, List<String> path) {
    return DirectoryLayer.getDefault().open(tx, path).join();
  }

  public static boolean doesSubdirectoryExists(Transaction tx, List<String> path) {
    return DirectoryLayer.getDefault().exists(tx, path).join();
  }

  public static void dropSubspace(Transaction tx, List<String> path) {
    DirectoryLayer.getDefault().remove(tx, path).join();
  }

  public static void removeKeyValuePair(Transaction tx, DirectorySubspace dir, Tuple keyTuple) {
    tx.clear(dir.pack(keyTuple));
  }

  public static Transaction openTransaction(Database db) {
    return db.createTransaction();
  }

  public static boolean commitTransaction(Transaction tx) {
    return tryCommitTx(tx, 0);
  }

  public static boolean tryCommitTx(Transaction tx, int retryCounter) {
    try {
      tx.commit().join();
      return true;
    } catch (FDBException e) {
      if (retryCounter < MAX_TRANSACTION_COMMIT_RETRY_TIMES) {
        retryCounter++;
        tryCommitTx(tx, retryCounter);
      } else {
        tx.cancel();
        return false;
      }
    }
    return false;
  }

  public static void abortTransaction(Transaction tx) {
    tx.cancel();
  }
}
