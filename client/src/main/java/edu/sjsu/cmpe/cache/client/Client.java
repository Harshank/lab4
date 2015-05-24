package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");

        CacheServiceInterface cache = new DistributedCacheService(
                new String[] {"http://localhost:3000",
                        "http://localhost:3001",
                        "http://localhost:3002" });
        cache.put(1, "a");
        System.out.println("put (1 => a)" );

        Thread.sleep(40000);

        cache.put(1, "b");
        System.out.println("put (1 => b)" );

        Thread.sleep(30000);
        String value = cache.get(1);
        System.out.println("get (1) => " + value);
        System.out.println("Exiting Cache Client...");
    }

}
