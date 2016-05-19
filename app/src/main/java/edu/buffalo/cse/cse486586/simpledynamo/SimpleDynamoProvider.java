package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static DBHelper dbh;
	private static HashMap<String,String> hm;
	private static SimpleDynamoProvider sdp = new SimpleDynamoProvider();
	private final Uri mUri;
	private final int SERVER_PORT = 10000;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static String successor = "";
	private static String predecessor = "";
	private static String myNodeId = "";
	private static String myEmulator = "";
	private static ArrayList<String> liveAvds;
	private static String[] queryVal = new String[2];
	private static HashMap<String,String> starResponse;
	private static int starResponseCount = 0;
	private final Lock queryLock = new ReentrantLock();
	private final Lock insertLock = new ReentrantLock();

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public SimpleDynamoProvider(){
		super();
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
		hm = new HashMap<String, String>(); //stores the hashed nodeIDs with the corresponding port numbers
		liveAvds = new ArrayList<String>(); //stores the list of available node ids
		starResponse = new HashMap<String, String>(); //stores the responses for @ queries
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String[] key = selection.split(",");
		if (key.length == 1) {
			Cursor cursor = dbh.getWritableDatabase().query(dbh.getDatabaseName(), null, "key=\"" + selection + "\"", selectionArgs, null, null, null);
			if (cursor.getCount() > 0) {
				Log.e("Deleted", selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection + ",del", "", successor, "DELETE", "F");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection + ",del", "", this.getSuccessor(successor), "DELETE", "F");
				return dbh.getWritableDatabase().delete(dbh.getDatabaseName(), "key=\"" + selection + "\"", selectionArgs);
			} else {
				String toSendAvd = this.getToSendAvd(selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection + ",del", "", toSendAvd, "DELETE", "F");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection + ",del", "", this.getSuccessor(toSendAvd), "DELETE", "F");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection + ",del", "", this.getSuccessor(this.getSuccessor(toSendAvd)), "DELETE", "F");
				return 0;
			}
		}
		else
			return dbh.getWritableDatabase().delete(dbh.getDatabaseName(), "key=\"" + key[0] + "\"", selectionArgs);
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {/*if this avd is the right place to insert then go ahead, else pass the query to the correct avd. The correct avd will then forward the request to two of its succsssors*/
		insertLock.lock();

		String keyI = (String) values.get(KEY_FIELD);
		String[] keyAr = keyI.split(","); //array contains two values : the key to be inserted and the hop id

		if(keyAr.length>1){ //if the hop id is attached to the key, we need to remove the hop id and recreate the content value
			ContentValues cv = new ContentValues();
			cv.put(KEY_FIELD,keyAr[0]);
			cv.put(VALUE_FIELD,(String)values.get(VALUE_FIELD));
			values = cv;
		}

		if(keyAr.length==1) {
			try {
				String keyHash = this.genHash(keyAr[0]);
				if (keyHash.compareTo(predecessor) > 0 && myNodeId.compareTo(keyHash) > 0) {
					dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(),null,values, SQLiteDatabase.CONFLICT_REPLACE);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",1", (String) values.get(VALUE_FIELD), successor, "INSERT", "F");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",2", (String) values.get(VALUE_FIELD),this.getSuccessor(successor),"INSERT","F");
					Log.v("insert2", values.toString());
				} else if (keyHash.compareTo(predecessor) > 0 && keyHash.compareTo(myNodeId) > 0 && myNodeId.equals(liveAvds.get(0))) {
					dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(),null,values, SQLiteDatabase.CONFLICT_REPLACE);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",1", (String) values.get(VALUE_FIELD), successor, "INSERT", "F");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",2", (String) values.get(VALUE_FIELD),this.getSuccessor(successor),"INSERT","F");
					Log.v("insert3", values.toString());
				} else if (keyHash.compareTo(myNodeId) < 0 && myNodeId.equals(liveAvds.get(0))) {
					dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(),null,values, SQLiteDatabase.CONFLICT_REPLACE);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",1", (String) values.get(VALUE_FIELD), successor, "INSERT", "F");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",2", (String) values.get(VALUE_FIELD),this.getSuccessor(successor),"INSERT","F");
					Log.v("insert4", values.toString());
				} else {
					String toSendAvd = this.getToSendAvd(keyAr[0]);
					if(toSendAvd.equals(myNodeId))
						dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(),null,values, SQLiteDatabase.CONFLICT_REPLACE);
					else
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",0", (String) values.get(VALUE_FIELD), toSendAvd, "INSERT", "F");

					String succ1 = this.getSuccessor(toSendAvd);
					if(succ1.equals(myNodeId))
						dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(),null,values, SQLiteDatabase.CONFLICT_REPLACE);
					else
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",1", (String) values.get(VALUE_FIELD),succ1,"INSERT","F");

					String succ2 = this.getSuccessor(succ1);
					if(succ2.equals(myNodeId))
						dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(),null,values, SQLiteDatabase.CONFLICT_REPLACE);
					else
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values.get(KEY_FIELD) + ",2", (String) values.get(VALUE_FIELD),succ2,"INSERT","F");
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		else{
			dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(), null, values, SQLiteDatabase.CONFLICT_REPLACE);
			Log.v("insert0", values.toString());
		}

		insertLock.unlock();
		return null;
	}

	private static boolean recoFlag = false;
	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		myEmulator = tel.getLine1Number().substring(7);

		queryLock.lock();
		recoFlag = true;

		try {
			myNodeId = this.genHash(myEmulator);
			Log.e(myEmulator, myNodeId);

			liveAvds.add(this.genHash("5554"));
			liveAvds.add(this.genHash("5556"));
			liveAvds.add(this.genHash("5558"));
			liveAvds.add(this.genHash("5560"));
			liveAvds.add(this.genHash("5562"));

			Collections.sort(liveAvds);

			predecessor = this.getPredecessor(myNodeId);
			successor = this.getSuccessor(myNodeId);

			hm.put(this.genHash("5554"),"11108"); //storing all the hashed nodeIds and the corresponding port numbers in the hashmap
			hm.put(this.genHash("5556"),"11112");
			hm.put(this.genHash("5558"),"11116");
			hm.put(this.genHash("5560"),"11120");
			hm.put(this.genHash("5562"),"11124");

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		/*int numRows = (int) DatabaseUtils.queryNumEntries(dbh.getReadableDatabase(), dbh.getDatabaseName());
		Log.e("Count",numRows + "");
		dbh.getWritableDatabase().execSQL("DELETE from KMDB");
		numRows = (int) DatabaseUtils.queryNumEntries(dbh.getReadableDatabase(), dbh.getDatabaseName());
		Log.e("Count1", numRows + "");*/

		/* Handling avd failure case
		* The node when restarts, will contact its successor and predecessor for the key value pairs
		* It will then stores the key-value pairs for which the node id is the actual one or the successor or the 2nd successor
		* */
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "@" + "," + myNodeId, "", successor, "IAMBACK","D");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "@" + "," + myNodeId, "", predecessor, "IAMBACK","D");

		try{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e){
			Log.e("ServerSocketError", "Can't create a ServerSocket");
		}

		dbh = new DBHelper(getContext());

		while(starResponseCount<2){}

		insertLock.lock();
		for(String key: starResponse.keySet()){
			String value = starResponse.get(key);
			key = key.substring(1,key.length()-1); //removing the square brackets
			value = value.substring(1,value.length()-1); //removing the square brackets
			String keyAr[] = key.split(", ");
			String valAr[] = value.split(", ");
			for(int i=0;i<keyAr.length;i++){
				String toSendAvd = this.getToSendAvd(keyAr[i]);
				if(!keyAr[i].equals("") && (myNodeId.equals(toSendAvd) || myNodeId.equals(this.getSuccessor(toSendAvd)) || myNodeId.equals(this.getSuccessor(this.getSuccessor(toSendAvd))))){
					Log.v("inserting", "failed key");
					ContentValues cv = new ContentValues();
					cv.put(KEY_FIELD, keyAr[i]); //,2 is added so that in the insert function it simply inserts the value and don't forward to other nodes
					cv.put(VALUE_FIELD, valAr[i]);
					dbh.getWritableDatabase().insertWithOnConflict(dbh.getDatabaseName(), null, cv, SQLiteDatabase.CONFLICT_REPLACE);
				}
			}
		}
		insertLock.unlock();
		queryLock.unlock();
		recoFlag = false;

		starResponse.clear();
		starResponseCount = 0;
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		queryLock.lock();
		Log.e("query",selection);

		String[] query = selection.split(","); //query[0] corresponds to the key and query[1] corresponds to the nodeId that requested the key to be queried

		if(selection.equals("*")){
			Cursor cursor = dbh.getWritableDatabase().rawQuery("select * from KMDB", null);

			for(String node: liveAvds){
				if(!node.equals(myNodeId))
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "@" + "," + myNodeId, "", node, "*QUERY","D");
			}
			while(starResponseCount<liveAvds.size()-1){}

			MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"});
			for(String key: starResponse.keySet()){
				String value = starResponse.get(key);
				key = key.substring(1,key.length()-1);
				value = value.substring(1,value.length()-1);
				String keyAr[] = key.split(", ");
				String valAr[] = value.split(", ");
				for(int i=0;i<keyAr.length;i++){
					cur.addRow(new Object[]{keyAr[i],valAr[i]});
				}
			}

			cursor.moveToFirst(); //add own data to the returning cursor as well
			for(int i=0;i<cursor.getCount();i++){
				String keyField = cursor.getString(0);
				String valField = cursor.getString(1);
				cur.addRow(new Object[]{keyField,valField});
				cursor.moveToNext();
			}

			starResponse.clear();
			starResponseCount = 0;
			queryLock.unlock();
			return cur;
		}
		else if(query[0].equals("@")){
			Cursor cursor = dbh.getWritableDatabase().rawQuery("select * from KMDB", null);
			if(query.length==1) {   //if the query request came this avd then directly return the cursor
				queryLock.unlock();
				return cursor;
			}
			else{
				cursor.moveToFirst();
				int len = cursor.getCount();
				ArrayList<String> keyA = new ArrayList<String>();
				ArrayList<String> valA = new ArrayList<String>();
				for(int i=0;i<len;i++){
					keyA.add(i,cursor.getString(0));
					valA.add(i, cursor.getString(1));
					cursor.moveToNext();
				}
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyA.toString(), valA.toString(), query[1], "*_RESP","F");
				queryLock.unlock();
				return null;
			}
		}
		else{
			Cursor cursor = dbh.getWritableDatabase().query(dbh.getDatabaseName(),projection,"key=\"" + query[0] + "\"",selectionArgs,sortOrder,null,null);

			if(cursor.getCount()==0 && query.length==1){
				String toSendAvd = this.getToSendAvd(query[0]);
				queryVal = new String[2];
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query[0] + "," + myNodeId, "", toSendAvd, "QUERY","E");

//				while(queryVal[1]==null){}
				boolean flag = false;
				while(flag == false){
					if(queryVal[0]!=null){
						if(queryVal[0].equals(query[0])&&queryVal[1]!=null&&!queryVal[1].equals("")){
							flag = true;
						}
					}
				}

				MatrixCursor cur1 = new MatrixCursor(new String[]{"key", "value"}, 1);
				cur1.addRow(queryVal);
				Log.e("response1",queryVal[0] + " " + queryVal[1]);
				queryVal = new String[2];
				queryLock.unlock();
				return cur1;
			}
			else if(query.length==1){ //if the query request came this avd then directly return the cursor
				cursor.moveToFirst();
				Log.e("response2",cursor.getString(1));
				queryLock.unlock();
				return cursor;
			}
			else{
				//while(cursor.getCount()==0){}
				if(recoFlag==true){//cursor.getCount()==0 ||
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query[0], "", query[1], "Q_RESP","F");
				}
				else{
					cursor.moveToFirst();
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, cursor.getString(0), cursor.getString(1), query[1], "Q_RESP","F");
				}
				queryLock.unlock();
				return null;
			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
		Log.e("Hash",input+" " + formatter.toString());
        return formatter.toString();
    }

	private String getToSendAvd(String key){
		String toSendAvd = "";
		try {
			String keyHash = this.genHash(key);
			int i=0;
			while(i<liveAvds.size()) {
				if(keyHash.compareTo(liveAvds.get(i))<0){
					toSendAvd = liveAvds.get(i);
					break;
				}
				i++;
			}
			if(i==5){
				toSendAvd = liveAvds.get(0);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return toSendAvd;
	}

	private String getSuccessor(String nodeId){
		int index = liveAvds.indexOf(nodeId);
		String succ = liveAvds.get((index + 1) % liveAvds.size());
		return succ;
	}

	private String getPredecessor(String nodeId){
		int index = liveAvds.indexOf(nodeId);
		String pre = "";
		if(index==0){
			pre = liveAvds.get(liveAvds.size()-1);
		}
		else {
			pre = liveAvds.get((index-1)%liveAvds.size());
		}
		return pre;
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... values) {
			String serverAvd = values[2];
			String remotePort = hm.get(serverAvd);
			boolean flag = false;
			int count = 0;
			while (flag==false) {
				count++;
				MessagePack forwardedPack = new MessagePack(myNodeId, values[0], values[1], values[3]);
				Log.e("CT",values[0]+" "+remotePort);
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
					socket.setSoTimeout(1000);
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(forwardedPack);
					out.close();
					socket.close();
					if(values[3].equals("QUERY")&&count<3){
						Log.e("Sending to","2nd Successor");
						serverAvd = sdp.getSuccessor(serverAvd);
						remotePort = hm.get(serverAvd);
						flag = false;
					}
					else
						flag = true;
				} catch (IOException e) {
					if(values[4].equals("D")){ //when avds are starting for the first time and it is trying to connect to its successor or predecessor and either or both are not alive or * query failure
						Log.e("Exception","Avd Restart Exception!");
						starResponseCount++;
						flag = true;
					}
					else if(values[4].equals("E")){
						if(count<3) {
							Log.e(values[0], "Query Failure Exception");
							serverAvd = sdp.getSuccessor(serverAvd);
							remotePort = hm.get(serverAvd);
						}
						else
							flag = true;
					}
					else if(values[4].equals("F")){
						flag = true; //insert or delete
					}
					else{
						Log.e("No idea what went wrong",values[2] + " " + values[0]);
						flag = true;
					}
					e.printStackTrace();
				}
			}
			return null;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while(true) {
				try {
					Socket clientSocket = serverSocket.accept();
					ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

					MessagePack incomingMsg = (MessagePack) in.readObject();
					in.close();

					Log.e("ST "+incomingMsg.msgType,incomingMsg.key+" "+incomingMsg.nodeId);
					if(incomingMsg.msgType.equals("INSERT")) {
						ContentValues cv = new ContentValues();
						cv.put(KEY_FIELD,incomingMsg.key);
						cv.put(VALUE_FIELD,incomingMsg.value);
						sdp.insert(mUri, cv);
					}
					else if(incomingMsg.msgType.equals("QUERY") || incomingMsg.msgType.equals("*QUERY") || incomingMsg.msgType.equals("IAMBACK")) {
						sdp.query(mUri, null, incomingMsg.key, null, "");
					}
					else if(incomingMsg.msgType.equals("Q_RESP")) {
						queryVal[0] = incomingMsg.key;
						queryVal[1] = incomingMsg.value;
					}
					else if(incomingMsg.msgType.equals("*_RESP")) {
						starResponse.put(incomingMsg.key,incomingMsg.value);
						starResponseCount++;
					}
					else if(incomingMsg.msgType.equals("DELETE")) {
						sdp.delete(mUri,incomingMsg.key,null);
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
