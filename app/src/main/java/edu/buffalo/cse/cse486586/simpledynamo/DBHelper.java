package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class DBHelper extends SQLiteOpenHelper {
    private static final int DATABASE_VERSION = 2;
    private static String TABLE_CREATE = "";
    private final String DBNAME = "KMDB";

    public DBHelper(Context context) {
        super(context, "KMDB", null, DATABASE_VERSION);
        TABLE_CREATE = "CREATE TABLE " + DBNAME + " ( key TEXT PRIMARY KEY unique, value TEXT );";
        Log.e("Table_Create", TABLE_CREATE);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS " + DBNAME);
        db.execSQL(TABLE_CREATE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}