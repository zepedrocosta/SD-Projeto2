package tukano.impl.java.servers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Blobs;
import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedShorts;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.data.Following;
import tukano.impl.java.servers.data.Likes;
import utils.DB;
import utils.Token;
import utils.kafka.KafkaSubscriber;
import utils.kafka.RecordProcessor;
import utils.kafka.SyncPoint;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
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
    private static final String BLOB_COUNT = "*";


    private static Logger Log = Logger.getLogger(JavaShortsReplicaAction.class.getName());
    private final ObjectMapper mapper = new ObjectMapper();


    public Result<String> createShort(Short value) {
        Result<Short> shrt = DB.insertOne(value);

        try {
            if (shrt.isOK())
                return ok(mapper.writeValueAsString(shrt.value()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return error(shrt.error());
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

            BlobsClients.get().delete(shrt.getBlobUrl(), Token.get());

        });
    }

    public Result<String> getShorts(String userId) {
        var query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
        try {
            return ok(mapper.writeValueAsString(DB.sql( query, String.class)));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return error(INTERNAL_ERROR);
    }

    public Result<String> follow(String userId1, String userId2, boolean isFollowing) {
        var f = new Following(userId1, userId2);
        Result<Following> res = isFollowing ? DB.insertOne(f) : DB.deleteOne(f);

        if (res.isOK())
            return ok("");

        return error(res.error());
    }

    public Result<List<String>> followers(String userId, String password) {
        return null;
    }

    public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
        return null;
    }

    public Result<List<String>> likes(String shortId, String password) {
        return null;
    }

    public Result<List<String>> getFeed(String userId, String password) {
        return null;
    }

    public Result<Void> deleteAllShorts(String userId, String password, String token) {
        return null;
    }

    static record Credentials(String userId, String pwd) {
        static JavaShorts.Credentials from(String userId, String pwd) {
            return new JavaShorts.Credentials(userId, pwd);
        }
    }

    protected final LoadingCache<JavaShorts.Credentials, Result<User>> usersCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(USER_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Result<User> load(JavaShorts.Credentials u) throws Exception {
                    var res = UsersClients.get().getUser(u.userId(), u.pwd());
                    if (res.error() == TIMEOUT)
                        return error(BAD_REQUEST);
                    return res;
                }
            });

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
                    var hits = DB.sql(QUERY, JavaShorts.BlobServerCount.class);

                    var candidates = hits.stream().collect(Collectors.toMap(JavaShorts.BlobServerCount::baseURI, JavaShorts.BlobServerCount::count));

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
                var servers = Discovery.getInstance().knownUrisOf(Blobs.NAME, 1);
                List<String> formattedURLs = new ArrayList<>();

                for (var server : servers)
                    formattedURLs.add(format("%s/%s/%s", server.toString(), Blobs.NAME, shortId));

                var blobURLs = new LinkedList<>(Arrays.asList(shrt.getBlobUrl().split("\\|")));
                Log.info("blobURL: " + shrt.getBlobUrl());
                Log.info(shortId + ": " + blobURLs);

                for (var url : blobURLs) {
                    Log.info("url: " + url);
                    if (!formattedURLs.contains(url)) {
                        blobURLs.remove(url);
                        var newUrl = getOtherUrl(url, shortId);
                        if (newUrl.equals("?"))
                            shrt.setBlobUrl(blobURLs.get(0));
                        else
                            shrt.setBlobUrl(blobURLs.get(0) + "|" + newUrl);
                        DB.updateOne(shrt);
                        break;
                    }
                }
                return ok(shrt);
            }

            return res;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    private String getOtherUrl(String blobUrl, String shortId) {
        try {
            var servers = blobCountCache.get(BLOB_COUNT);

            var leastLoadedServer = servers.entrySet()
                    .stream()
                    .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                    .findFirst();

            if (leastLoadedServer.isPresent()) {
                var newUrl = format("%s/%s/%s", leastLoadedServer.get().getKey(), Blobs.NAME, shortId);

                if (!newUrl.equals(blobUrl)) {
                    servers.compute(leastLoadedServer.get().getKey(), (k, v) -> v + 1L);
                    return newUrl;
                } else {
                    try {
                        leastLoadedServer = servers.entrySet()
                                .stream()
                                .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                                .skip(1)
                                .findFirst();

                        if (leastLoadedServer.isPresent()) {
                            var uri = leastLoadedServer.get().getKey();
                            servers.compute(uri, (k, v) -> v + 1L);
                            return format("%s/%s/%s", uri, Blobs.NAME, shortId);
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

}
