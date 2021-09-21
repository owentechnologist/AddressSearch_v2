package com.redislabs.sa.ot.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStreamAdapter {

    private JedisPool connectionPool;
    private String streamName;

    public RedisStreamAdapter(String streamName, JedisPool connectionPool){
        this.connectionPool=connectionPool;
        this.streamName=streamName;
    }

    public void listenToStream(StreamEventMapProcessor streamEventMapProcessor){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try (Jedis streamListener =  connectionPool.getResource();){
                        String key = "";
                        List<StreamEntry> streamEntryList = null;
                        StreamEntry value = null;
                        StreamEntryID nextID = new StreamEntryID();
                        System.out.println("main.kickOffStreamListenerThread: Actively Listening to Stream "+streamName);
                        Map.Entry<String, StreamEntryID> streamQuery = null;
                        while(true){
                            streamQuery = new AbstractMap.SimpleImmutableEntry<>(
                                    streamName, nextID);
                            List<Map.Entry<String, List<StreamEntry>>> streamResult =
                                    streamListener.xread(1,Long.MAX_VALUE,streamQuery);// <--  has to be Long.MAX_VALUE to work
                            key = streamResult.get(0).getKey(); // name of Stream
                            streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update

                            value = streamEntryList.get(0);// entry written to stream
                            // the following formats are copacetic:
                            //1621843705891-0 {type2:phone={"event": "set", "key": "type2:phone", "type": "string", "value": "312-7777"}}
                            //1621847672478-0 {originalCityName=cokwitlum, requestID=PM_UID76863937290053, bestMatch=Coquitlam}
                            //1621847941565-0 {type2:hash1={"event": "hset", "key": "type2:hash1", "type": "hash", "value": {"name": "bob", "phone": "212-555-1213"}}}
                            //1621852546017-0 {type2:list1={"event": "lpush", "key": "type2:list1", "type": "list", "value": ["sue", "mary", "bob", "mary"]}}
                            System.out.println("StreamListenerThread: received... "+key+" "+value);
                            Map<String,StreamEntry> entry = new HashMap();
                            entry.put(key+":"+value.getID(),value);
                            streamEventMapProcessor.processStreamEventMap(entry);
                            nextID = value.getID();
                        }
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }).start();

    }

}
