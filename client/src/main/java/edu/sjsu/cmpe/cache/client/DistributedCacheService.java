package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Distributed cache service
 *
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String [] cacheServerUrls;

    public DistributedCacheService(String [] serverUrls) {
        this.cacheServerUrls = serverUrls;
    }

    @Override
    public String get(long key) {
        Map<String, List<String>> valueServerMap = new HashMap<String, List<String>>();

        Map<String, Future<HttpResponse<JsonNode>>> futures = new HashMap<String, Future<HttpResponse<JsonNode>>>();

        for (String cacheServerUrl : cacheServerUrls) {
            Future<HttpResponse<JsonNode>> future = Unirest.get(cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync();
            futures.put(cacheServerUrl, future);
        }

        // Repair and Read
        String maxValue = null;
        int maxValueCount = 0;
        for (Entry<String, Future<HttpResponse<JsonNode>>> future : futures.entrySet()) {
            try {
                HttpResponse<JsonNode> response = future.getValue().get();
                if (response.getCode() == 200) {
                    String value = response.getBody().getObject().getString("value");
                    List<String> serversWithThisValue = null;
                    if (valueServerMap.containsKey(value)) {
                        serversWithThisValue = valueServerMap.get(value);
                    } else {
                        serversWithThisValue = new ArrayList<String>();
                        valueServerMap.put(value, serversWithThisValue);
                    }
                    serversWithThisValue.add(future.getKey());
                    int count = serversWithThisValue.size();
                    if (count > maxValueCount) {
                        maxValueCount = count;
                        maxValue = value;
                    }

                }
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        if (maxValueCount < cacheServerUrls.length / 2) {
            throw new RuntimeException("Quorum not reached for value of this key. "
                    + " Cluster might be unstable");
        }

        for (Entry<String, List<String>> valueServersPair : valueServerMap.entrySet()) {
            String value = valueServersPair.getKey();
            if (!maxValue.equals(value)) {
                for (String cacheServerUrl : valueServersPair.getValue()) {
                    HttpResponse<JsonNode> response = null;
                    try {
                        response = Unirest
                                .put(cacheServerUrl + "/cache/{key}/{value}")
                                .header("accept", "application/json")
                                .routeParam("key", Long.toString(key))
                                .routeParam("value", maxValue).asJson();
                    } catch (UnirestException e) {
                        System.err.println(e);
                    }

                    if (response.getCode() != 200) {
                        System.out.println("Failed to update the cache.");
                    }
                }
            }
        }

        // Return quorum value
        return maxValue;
    }

    @Override
    public void put(long key, String value) {
        Map<String, Future<HttpResponse<JsonNode>>> futures = new HashMap<String, Future<HttpResponse<JsonNode>>>();

        for (String cacheServerUrl : cacheServerUrls) {
            Future<HttpResponse<JsonNode>> future = Unirest.put(cacheServerUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value)
                    .asJsonAsync();
            futures.put(cacheServerUrl, future);
        }

        int serversSuccessfullyUpdate = 0;

        for (Entry<String, Future<HttpResponse<JsonNode>>> future : futures.entrySet()) {
            try {
                HttpResponse<JsonNode> response = future.getValue().get();
                if (response.getCode() == 200) {
                    serversSuccessfullyUpdate ++;
                } else {
                    System.out.println("Failed to add to the cache.");
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        if (serversSuccessfullyUpdate < cacheServerUrls.length / 2) {
            delete(key);
        }
    }

    @Override
    public void delete(long key) {
        // Delete the key on all servers in cluster
        for (String cacheServerUrl : this.cacheServerUrls) {
            HttpResponse<JsonNode> response = null;
            try {
                response = Unirest.delete(cacheServerUrl + "/cache/{key}")
                        .header("accept", "application/json")
                        .routeParam("key", Long.toString(key)).asJson();
            } catch (UnirestException e) {
                System.err.println(e);
            }

            if (response.getCode() != 200) {
                System.out.println("Failed to delete from the cache.");
            }
        }
    }
}