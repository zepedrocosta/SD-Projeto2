package tukano.impl.java.servers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Blobs;
import tukano.api.java.Result;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.data.Following;
import tukano.impl.java.servers.data.Likes;
import utils.DB;
import utils.Hash;
import utils.Token;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static tukano.api.java.Result.*;
import static tukano.api.java.Result.ErrorCode.*;
import static tukano.impl.java.clients.Clients.BlobsClients;
import static tukano.impl.java.clients.Clients.UsersClients;
import static utils.DB.getOne;

public class JavaShortsReplicaAction {

    private static final long USER_CACHE_EXPIRATION = 3000;
    private static final long SHORTS_CACHE_EXPIRATION = 3000;
    private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;
    private static final String BLOBS_URL = "%s?timestamp=%s&verifier=%s";
    private static final String TOKEN = "123456";
    private static final String BLOB_COUNT = "*";


    private static Logger Log = Logger.getLogger(JavaShortsReplicaAction.class.getName());
    private final ObjectMapper mapper = new ObjectMapper();


    public Result<String> createShort(Short value) {
        Log.info(() -> format("createShort : userId = %s\n", value.getOwnerId()));

        try {
            var blobURLs = buildBlobsURLs(value);
            value.setBlobUrl(blobURLs);
            Result<Short> shrt = DB.insertOne(value);
            if (shrt.isOK())
                return ok(mapper.writeValueAsString(shrt.value()));
            return error(shrt.error());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return error(INTERNAL_ERROR);
    }

    public Result<String> getShort(String shortId) {
        Log.info(() -> format("getShort : shortId = %s\n", shortId));

        var res = shortFromCache(shortId);

        if (res.isOK())
            try {
                return ok(mapper.writeValueAsString(res.value()));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        return error(res.error());
    }

    public Result<String> deleteShort(Short shrt) {
        var shortId = shrt.getShortId();
        Log.info(() -> format("deleteShort : shortId = %s\n", shortId));

        return DB.transaction(hibernate -> {

            shortsCache.invalidate(shortId);
            hibernate.remove(shrt);

            var query = format("SELECT * FROM Likes l WHERE l.shortId = '%s'", shortId);
            hibernate.createNativeQuery(query, Likes.class).list().forEach(hibernate::remove);

            //BlobsClients.get().delete(shrt.getBlobUrl(), Token.get()); NOT WORKING

        });
    }

    public Result<String> getShorts(String userId) {
        Log.info(() -> format("getShorts : userId = %s\n", userId));

        var query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
        try {
            return ok(mapper.writeValueAsString(DB.sql( query, String.class)));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return error(INTERNAL_ERROR);
    }

    public Result<String> follow(String userId1, String userId2, boolean isFollowing) {
        Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s\n", userId1, userId2, isFollowing));

        var f = new Following(userId1, userId2);
        Result<Following> res = isFollowing ? DB.insertOne(f) : DB.deleteOne(f);

        if (res.isOK())
            return ok("");

        return error(res.error());
    }

    public Result<String> followers(String userId) {
        Log.info(() -> format("followers : userId = %s\n", userId));

        var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);
        try {
            return ok(mapper.writeValueAsString(DB.sql( query, String.class)));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return error(INTERNAL_ERROR);
    }

    public Result<String> like(String shortId, String userId, boolean isLiked, String ownerId) {
        Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s\n", shortId, userId, isLiked));

        shortsCache.invalidate( shortId );

        var l = new Likes(userId, shortId, ownerId);
        Result<Likes> res = isLiked ? DB.insertOne(l) : DB.deleteOne(l);

        if (res.isOK())
            return ok("");

        return error(res.error());
    }

    public Result<String> likes(String shortId) {
        var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);
        try {
            return ok(mapper.writeValueAsString(DB.sql( query, String.class)));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return error(INTERNAL_ERROR);
    }

    public Result<String> getFeed(String userId) {
        Log.info(() -> format("getFeed : userId = %s\n", userId));

        final var QUERY_FMT = """
				SELECT s.shortId, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.shortId, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";

        try {
            return ok(mapper.writeValueAsString(DB.sql( format(QUERY_FMT, userId, userId), String.class)));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return error(INTERNAL_ERROR);
    }

    public Result<String> deleteAllShorts(String userId, String password) {
        Log.info(() -> format("deleteAllShorts : userId = %s, password = %s\n", userId, password));

        return DB.transaction( (hibernate) -> {

            usersCache.invalidate( new Credentials(userId, password) );

            //delete shorts
            var query1 = format("SELECT * FROM Short s WHERE s.ownerId = '%s'", userId);
            hibernate.createNativeQuery(query1, Short.class).list().forEach( s -> {
                shortsCache.invalidate( s.getShortId() );
                DB.deleteOne(s);
            });

            //delete follows
            var query2 = format("SELECT * FROM Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);
            hibernate.createNativeQuery(query2, Following.class).list().forEach( hibernate::remove );

            //delete likes
            var query3 = format("SELECT * FROM Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);
            hibernate.createNativeQuery(query3, Likes.class).list().forEach( l -> {
                shortsCache.invalidate( l.getShortId() );
                DB.deleteOne(l);
            });

        });
    }

    static record Credentials(String userId, String pwd) {
        static Credentials from(String userId, String pwd) {
            return new Credentials(userId, pwd);
        }
    }

    protected final LoadingCache<Credentials, Result<User>> usersCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(USER_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Result<User> load(Credentials u) throws Exception {
                    var res = UsersClients.get().getUser(u.userId(), u.pwd());
                    if (res.error() == TIMEOUT)
                        return error(BAD_REQUEST);
                    return res;
                }
            });

    static record BlobServerCount(String baseURI, Long count) {
    }

    protected final LoadingCache<String, Result<Short>> shortsCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(SHORTS_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Result<Short> load(String shortId) throws Exception {

                    var query = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
                    var likes = DB.sql(query, Long.class);
                    return errorOrValue(getOne(shortId, Short.class), shrt -> shrt.copyWith(likes.get(0)));
                }
            });

    protected final LoadingCache<String, Map<String, Long>> blobCountCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(BLOBS_USAGE_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Map<String, Long> load(String __) throws Exception {
                    final var QUERY = "SELECT REGEXP_SUBSTRING(s.blobUrl, '^(\\w+:\\/\\/)?([^\\/]+)\\/([^\\/]+)') AS baseURI, count('*') AS usage From Short s GROUP BY baseURI";
                    var hits = DB.sql(QUERY, BlobServerCount.class);

                    var candidates = hits.stream().collect(Collectors.toMap(BlobServerCount::baseURI, BlobServerCount::count));

                    for (var uri : BlobsClients.all())
                        candidates.putIfAbsent(uri.toString(), 0L);

                    return candidates;

                }
            });


    protected Result<Short> shortFromCache(String shortId) {
        try {
            var res = shortsCache.get(shortId);
            Short shrt;

            if (res.isOK()) {
                shrt = res.value();
                shortsCache.invalidate(shortId);
                var servers = Discovery.getInstance().knownUrisOf(Blobs.NAME, 1);
                List<String> formattedURLs = new ArrayList<>();

                for (var server : servers)
                    formattedURLs.add(format("%s/%s/%s", server.toString(), Blobs.NAME, shortId));

                var blobURLs = new LinkedList<>(Arrays.asList(shrt.getBlobUrl().split("\\|")));
                Log.info(shortId + ": " + blobURLs + "\n");

                for (var url : blobURLs) {
                    if (!formattedURLs.contains(url.split("\\?")[0])) {
                        // Wasn't able to invalidate only one, so I'm invalidating them all
                        blobCountCache.invalidateAll();
                        blobURLs.remove(url);

                        var newUrl = getOtherUrl(blobURLs.get(0), url, shortId);
                        if (newUrl.equals("?"))
                            shrt.setBlobUrl(blobURLs.get(0));
                        else
                            shrt.setBlobUrl(blobURLs.get(0) + "|" + newUrl);

                        break;
                    }
                }

                var tmp = buildBlobsURLs(shrt);
                shrt.setBlobUrl(tmp);
                DB.updateOne(shrt);
                return ok(shrt);
            }

            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    private String getOtherUrl(String blobUrl, String removedUrl, String shortId) {
        try {
            var servers = blobCountCache.get(BLOB_COUNT);

            var leastLoadedServer = servers.entrySet()
                    .stream()
                    .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                    .findFirst();

            if (leastLoadedServer.isPresent()) {
                var newUrl = format("%s/%s/%s", leastLoadedServer.get().getKey(), Blobs.NAME, shortId);

                if (!newUrl.equals(blobUrl.split("\\?")[0]) && !newUrl.equals(removedUrl.split("\\?")[0])) {
                    servers.compute(leastLoadedServer.get().getKey(), (k, v) -> v + 1L);
                    return newUrl;
                }
                else {
                    try {
                        leastLoadedServer = servers.entrySet()
                                .stream()
                                .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                                .skip(1)
                                .findFirst();


                        if (leastLoadedServer.isPresent()) {
                            newUrl = format("%s/%s/%s", leastLoadedServer.get().getKey(), Blobs.NAME, shortId);
                        }


                        if (!newUrl.equals(blobUrl.split("\\?")[0]) && !newUrl.equals(removedUrl.split("\\?")[0])) {
                            servers.compute(leastLoadedServer.get().getKey(), (k, v) -> v + 1L);
                            return newUrl;
                        }
                        else {
                            leastLoadedServer = servers.entrySet()
                                    .stream()
                                    .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                                    .skip(2)
                                    .findFirst();


                            if (leastLoadedServer.isPresent()) {
                                var uri = leastLoadedServer.get().getKey();
                                return format("%s/%s/%s", uri, Blobs.NAME, shortId);
                            }
                        }

                    } catch (Exception x) {
                        return "?";
                    }
                }
            }

        } catch (Exception x) {
            x.printStackTrace();
        }
        return "?";
    }

    private String buildBlobsURLs(Short shrt) {
		String[] servers = shrt.getBlobUrl().split("\\|");
		System.out.println("servers: " + servers[0] + " " + servers[1]);
		String[] parts = servers[0].split("\\?");
		var timeLimit = System.currentTimeMillis() + 10000;
		var blobURLs = new StringBuilder(format(BLOBS_URL, parts[0],
				timeLimit, getVerifier(timeLimit, parts[0].toString())));
		String[] parts2 = servers[1].split("\\?");
		blobURLs.append(format("|" + BLOBS_URL, parts2[0], timeLimit,
				getVerifier(timeLimit, parts2[0])));
		System.out.println("blobURLs: " + blobURLs.toString());
		return blobURLs.toString();
	}

    private static String getVerifier(long timelimit, String blobUrl) {
		String ip = blobUrl.substring(blobUrl.indexOf("://") + 3, blobUrl.lastIndexOf(":"));
		return Hash.md5(ip, String.valueOf(timelimit), TOKEN);
	}
}
