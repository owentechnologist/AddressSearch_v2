package com.redislabs.sa.ot.addresssearch;


import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import io.redisearch.client.IndexDefinition;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

public class Main {
    static final String XMARK = "XXXXX";
    static final String addrIndex = "IDX:ADDRESSES";
    static final String DATA_UPDATES_STREAM = "X:FOR_PROCESSING{sgc}";
    static Jedis connection = null;
    static final JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
    static Main main = null;

    public static void main(String [] args){
        main = new Main();
        System.out.println("cleanupIDX: ");
        main.cleanupIDX();
        System.out.println("prepCities: ");
        main.prepCities();
        System.out.println("prepIDX: ");
        main.prepIndex();
        main.runQueryScoreTest();
        RedisStreamAdapter streamAdapter = new RedisStreamAdapter(DATA_UPDATES_STREAM,jedisPool);
        TestStreamEventMapProcessor streamEventMapProcessor =new TestStreamEventMapProcessor();
        streamEventMapProcessor.setCallbackTarget(main);
        streamAdapter.listenToStream(streamEventMapProcessor);
    }

    void runQueryScoreTest(){
        System.out.println("Issue TEST SCORER Queries: ");
        //1112:
        main.addrSearch("San Francisco","CA","94016","USA");
        //1012:
        main.addrSearch("San Francisco","CA","","USA");
        //1002:
        main.addrSearch("san francisco","","","");
        //1002:  (not sure how the mispelled word becomes a tag...)
        main.addrSearch("san fransisko","","","");
        //111:
        main.addrSearch("","CA","94016","");
        //101:
        main.addrSearch("","","94016","");
        //11:
        main.addrSearch("","CA","","");
        //1:
        main.addrSearch("","","","");
        //0 (no match)
        main.addrSearch("Bogus Nutcase","CA","94016","USA");
    }

    void prepIndex(){
        Schema sc = new Schema()
                .addField(new Schema.TextField("city", 1000.0, false, false, false, "dm:en"))
                .addTextField("state_or_province", 10.0)
                .addTextField("zip_codes_or_postal_codes",100.0)
                .addTextField("country",1.0)
                .addTagField("city_as_tag");
        IndexDefinition def = new IndexDefinition()
                .setPrefixes(new String[] {"addr:"});
        Client client = new Client(addrIndex,jedisPool);
        client.createIndex(sc, Client.IndexOptions.defaultOptions().setDefinition(def));
    }

    void prepCities() {
        Map<String, String> map = new HashMap<String, String>();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            map.put("city_as_tag", "SanFrancisco");
            map.put("city", "SanFrancisco");
            map.put("state_or_province", "CA");
            map.put("country", "USA");
            map.put("zip_codes_or_postal_codes", "94016");
            jedis.hset("addr:sfo", map);

            map.put("city_as_tag", "SanJose");
            map.put("city", "SanJose");
            map.put("state_or_province", "CA");
            map.put("country", "USA");
            map.put("zip_codes_or_postal_codes", "94088");
            jedis.hset("addr:sj", map);


            map.put("city_as_tag", "LosAngeles");
            map.put("city", "LosAngeles");
            map.put("state_or_province", "CA");
            map.put("country", "USA");
            map.put("zip_codes_or_postal_codes", "90001");
            jedis.hset("addr:la", map);

            map.put("city_as_tag", "Albuquerque");
            map.put("city", "Albuquerque");
            map.put("state_or_province", "NM");
            map.put("country", "USA");
            map.put("zip_codes_or_postal_codes", "87101");
            jedis.hset("addr:abq", map);
            // note in the args to the script below an unused key is provided with
            // a routing value between the {} which is all redis needs
            // to ensure the script is fired in the correct shard
            // redis search does not need all the hashes to be in the same shard
            // the lua script does.
            // comment out the lua script and use the for loop below it
            // to write hashes to all the shards if you prefer
            jedis.eval("for index = 1,10000,1 do redis.call('HSET','addr:{sgc}:'..index,'city',string.char((index%20)+97) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) ..'SomeCity'..index,'city_as_tag',string.char((index%20)+97) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) .. 'SomeCity'..index,'state_or_province','CA','country','USA','zip_codes_or_postal_codes',''.. ((index%500)*10)+(88999)) end" ,1, "s:cityList{gS}" );
            /*
            This loop will write address hashes to all shards.
            This is because no routing value is provided as part of the keyname
            This is done much slower than when using the LUA script above ^
            for(int x = 0;x<10000;x++){
                map.put("city", "gSomeCity"+x);
                map.put("city_as_tag", "gSomeCity"+x);
                map.put("state_or_province", "NM");
                map.put("country", "USA");
                map.put("zip_codes_or_postal_codes", ""+((x%500)*10)+(83023)));
                jedis.hset("addr:"+x, map);
            }*/
        } catch (Throwable t) {
            System.out.println("WARNING: ");
            t.printStackTrace();
        }finally{jedis.close();}
    }

    void cleanupIDX(){
        try{
            Client client = new Client(addrIndex,jedisPool);
            client.dropIndex();
        }catch(Throwable t){
            System.out.println("WARNING: ");
            t.printStackTrace();}
    }

    /*
    When using DISMAX as scorer:
    Scores for matched cities range from 1 to 1112
    (Unmatched score 0)
    Each order of magnitude indicates a diff attribute provided
    if the tag field is matched then the last digit becomes a 2
    CITY ZIP STATE COUNTRY
    1    1    1    1
    1    1    0    1
    1    0    1    1
    1    0    0    1
    0    1    1    1
    0    1    0    1
    0    0    1    1
    0    0    0    1
    */
    //Note that this method only returns the top-scoring result for a query:
    public Document addrSearch(String city, String state, String zip,String country){
        //print(city,' ', state, ' ', zip_postal)
        System.out.println("addrSearch called with: "+city+"  "+state+"  "+zip+"  "+country);

        if (null == city || city == "") {
            city = XMARK;
        }else {
            city = city.replaceAll("[^a-zA-Z0-9]","");
        }
        if (null == state || state == "") {
            state = XMARK;
        }else {
            state = state.replaceAll("[^a-zA-Z0-9]","");
        }
        if (null == zip || zip == "") {
            zip = XMARK;
        }else {
            zip = zip.replaceAll("[^a-zA-Z0-9]","");
        }
        if (null == country || country == "") {
            country = "USA";
        }else {
            country = country.replaceAll("[^a-zA-Z0-9]","");
        }
        if(country.equalsIgnoreCase("USA") && zip.length()>5){
            zip = zip.substring(0,5);
        }
        System.out.println("addrSearch cleaned args = : "+city+"  "+state+"  "+zip+"  "+country);

        Client client = new Client(addrIndex,jedisPool);
        //ternary booleanExpression ? (DO TRUE) : (DO FALSE)
        String countryQuery = country.equalsIgnoreCase(XMARK)?"":" ~@country:(" + country+")";
        String stateQuery = state.equalsIgnoreCase(XMARK)?"":" @state_or_province:("  + state+")";
        String zipQuery = zip.equalsIgnoreCase(XMARK)?"":"  @zip_codes_or_postal_codes:(" + zip+")";
        String cityQuery = city.equalsIgnoreCase(XMARK)?"":" @city:(" + city + "|"+ city + "*|%"+ city + "%)";
        String cityTagQuery = city.equalsIgnoreCase(XMARK)?"":" ~@city_as_tag:{" + city + "}";
        String queryString = zipQuery+cityQuery+stateQuery+cityTagQuery+countryQuery;
        String scorer = "DISMAX";//"TFIDF.DOCNORM"; //"DISMAX" "DOCSCORE"
        if((!(zipQuery.equalsIgnoreCase("")))&&(!(cityQuery.equalsIgnoreCase("")))){ scorer = "DISMAX";}
        Query q = new Query(queryString).setScorer(scorer).setWithScores().limit(0,2);

        SearchResult result = client.search(q);                                           //TFIDF.DOCNORM
        System.out.println("\n QUERY: FT.SEARCH "+addrIndex+" \""+queryString+"\"  SCORER "+scorer+" WITHSCORES");
        System.out.println("addrSearch redisSearch got this many results: "+result.totalResults);
        for(Document doc :result.docs){
            System.out.println(doc.getScore());
            System.out.println(doc.toString());
        }
        Document addressFound = null;
        if((!(null == result))&&(result.totalResults>0)){
            addressFound = result.docs.get(0);
        }
        return addressFound;
    }

}
