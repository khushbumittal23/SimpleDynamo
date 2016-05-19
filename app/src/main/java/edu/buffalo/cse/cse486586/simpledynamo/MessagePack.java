package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class MessagePack implements Serializable {
    protected String nodeId;
    protected String key;
    protected String value;
    protected String msgType;

    MessagePack(String nodeId, String key, String value, String msgType){
        this.nodeId = nodeId;
        this.key = key;
        this.value = value;
        this.msgType = msgType;
    }

}
