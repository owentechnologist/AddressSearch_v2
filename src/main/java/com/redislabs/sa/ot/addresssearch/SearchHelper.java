package com.redislabs.sa.ot.addresssearch;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import redis.clients.jedis.JedisPool;

/**
 * A class that is responsible for searching for addresses
 * The act of searching is getting more involved - so breaking it out into it's own class
 */
public class SearchHelper {

    static final String XMARK = "XXXXX";
    static final String addrIndex = "IDX:ADDRESSES";
    static final JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();

    public Document addrSearch(String city, String zip, String state, String country){
        //print(city,' ', state, ' ', zip_postal)
        System.out.println("SearchHelper.addrSearch called with: "+city+"  "+zip+"  "+state+"  "+country);
        city = cleanString(city);
        state = cleanString(state);
        zip = cleanString(zip);
        country = cleanString(country);

        if(country.equalsIgnoreCase("USA") && zip.length()>5){
            zip = zip.substring(0,5);
        }

        System.out.println("SearchHelper.addrSearch cleaned args = : "+city+"  "+zip+"  "+state+"  "+country);

        // BREAK into two separate searches:
        // first uses tag - if result - don't do second search
        // use DISMAX to determine which of the tags matched so they can be used for second query:
        String scorer = "DISMAX";
        String queryString = getTagQuery(city,zip,state,country);
        // get 1 result only - no need for more if all tags match it will be an exact hit:
        Document doc = executeQuery(queryString,scorer);
        // if some tags do not match then the score determines next query
        Double score = 0.0;
        score = doc.getScore();
        System.out.println(score);
        System.out.println("\t\tTAG QUERY RESULT: \n\n"+doc.toString());

        // if score is 1115 assume exact match and no need to execute follow up query based on Text
        if(score < 1111.0 ){
            queryString = getTextQuery(score,city,zip,state,country);
            scorer = "TFIDF.DOCNORM"; // "DOCSCORE" //"DISMAX";
            doc = executeQuery(queryString,scorer);
            score = doc.getScore();
            System.out.println(score);
            System.out.println("\n\t\tTEXT QUERY RESULT: \n\n"+doc.toString());
        }
        return doc;
    }

    String cleanString(String input){
        if (null == input || input == "") {
            input = XMARK;
        }else {
            input = input.replaceAll("[^a-zA-Z0-9]","");
        }
        return input;
    }

    //reusable search method regardless of query or scorer:
    Document executeQuery(String queryString,String scorer){
        Client client = new Client(addrIndex,jedisPool);
        Query q = new Query(queryString).setScorer(scorer).setWithScores().limit(0,1);
        SearchResult result = client.search(q);
        System.out.println("\n\tQUERY: FT.SEARCH "+addrIndex+" \""+queryString+"\"  SCORER "+scorer+" WITHSCORES");
        System.out.println("SearchHelper.addrSearch got this many results: \n>>>>>> "+result.totalResults+" <<<<<<");
        Document addressFound = null;
        if(result.totalResults>0) {
            for (Document doc : result.docs) {
                addressFound = doc;
                addressFound.set("totalResults", result.totalResults);
            }
        }else{
            addressFound = new Document("noresult",0.0);
        }
        return addressFound;
    }

    String getTagQuery(String city,String zip,String state,String country) {
        //ternary booleanExpression ? (DO TRUE) : (DO FALSE)
        String cityQuery = city.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.cityAttribute+":(" + city + "|"+ city + "*|%"+ city + "%)";
        String zipQuery = zip.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.zipAttribute+":(" + zip+")";
        String stateQuery = state.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.stateAttribute+":("  + state+")";
        String countryQuery = country.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.countryAttribute+":(" + country+")";

        String cityTagQuery = city.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.cityTagAttribute+":{" + city + "}";
        String zipTagQuery = zip.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.zipTagAttribute+":{" + zip+"}";
        String stateTagQuery = state.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.stateTagAttribute+":{"  + state+"}";
        String countryTagQuery = country.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.countryTagAttribute+":{" + country+"}";
        String queryString = "("+cityTagQuery+zipTagQuery+stateTagQuery+countryTagQuery+") | ( "+stateQuery+countryQuery+zipQuery+cityQuery+" )";
        return queryString;
    }

    /*
    DISMAX is only usable when exact matches are in play
    For example if searching using tags
    When using DISMAX as scorer:
    Scores for matched cities range from 1 to 1112
    (Unmatched score 0)
    Each order of magnitude indicates a diff attribute provided
    if any tag field is matched then the last digit is incremented for each tag matched
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
    String getTextQuery(double score, String city,  String zip, String state, String country){

        String cityTagQuery = city.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.cityTagAttribute+":{" + city + "}";
        String zipTagQuery = zip.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.zipTagAttribute+":{" + zip+"}";
        String stateTagQuery = state.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.stateTagAttribute+":{"  + state+"}";
        String countryTagQuery = country.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.countryTagAttribute+":{" + country+"}";

        //ternary booleanExpression ? (DO TRUE) : (DO FALSE)
        String cityQuery = city.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.cityAttribute+":(" + city + "|"+ city + "*|%"+ city + "%)";
        String zipQuery = zip.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.zipAttribute+":(" + zip+ "|"+ zip + "*|%"+ zip + "%)";
        String stateQuery = state.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.stateAttribute+":("  + state+"|"+ state + "*|%"+ state + "%)";
        String countryQuery = country.equalsIgnoreCase(XMARK)?"":" @"+AddressSchemaConstants.countryAttribute+":(" + country+"|"+ country + "*|%"+ country + "%)";
        String queryString = "";
        //based on the score - build the query
        // use tag for city zip and state and text for country
        int decider = (int)score;
        System.out.println("decider score == "+decider);
        switch(decider) {
            case 1110: // city zip and state tags matched
                queryString = cityTagQuery+zipTagQuery+stateTagQuery+countryQuery;
                break;
            case 1101: // city zip and country tags matched
                queryString = cityTagQuery+zipTagQuery+stateQuery+countryTagQuery;
                break;
            case 1011: // city state and country tags matched
                queryString = cityTagQuery+zipQuery+stateTagQuery+countryTagQuery;
                break;
            case 111: //  zip state and country tags matched
                queryString = cityQuery+zipTagQuery+stateTagQuery+countryTagQuery;
                break;
            case 1100: // City and zip tags matched
                queryString = cityTagQuery+zipTagQuery+stateQuery+countryQuery;
                break;
            case 1010: // City and state tags matched
                queryString = cityTagQuery+zipQuery+stateTagQuery+countryQuery;
                break;
            case 1001: // City and Country tags matched
                queryString = cityTagQuery+zipQuery+stateQuery+countryTagQuery;
                break;
            case 1000: // only city tag matched
                queryString = cityTagQuery+zipQuery+stateQuery+countryQuery;
                break;
            case 110: // zip and state tags matched
                queryString = cityQuery+zipTagQuery+stateTagQuery+countryQuery;
                break;
            case 101: // zip and country tags matched
                queryString = cityQuery+zipTagQuery+stateQuery+countryTagQuery;
                break;
            case 11: // state and country tags matched
                queryString = cityQuery+zipQuery+stateTagQuery+countryTagQuery;
                break;
            case 100: // only zip tag matched
                queryString = cityQuery+zipTagQuery+stateQuery+countryQuery;
                break;
            case 10: // only state tag matched
                queryString = cityQuery+zipQuery+stateTagQuery+countryQuery;
                break;
            case 1: //only country tag matched
                queryString = cityQuery+zipQuery+stateQuery+countryTagQuery;
                break;
            default : //only country tag matched
                queryString = cityQuery+zipQuery+stateQuery+countryQuery;
        }
        return queryString;
    }

}
