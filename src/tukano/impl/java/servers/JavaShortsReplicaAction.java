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
import utils.DB;
import utils.kafka.KafkaSubscriber;
import utils.kafka.RecordProcessor;
import utils.kafka.SyncPoint;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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

    public Result<List<String>> getShorts(String userId) {
        return null;
    }

    public Result<Void> deleteShort(String shortId, String password) {
        return null;
    }

    public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
        return null;
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
                    return errorOrValue( getOne(shortId, Short.class), shrt -> shrt.copyWith( likes.get(0) ) );
                }
            });

    protected final LoadingCache<String, Map<String,Long>> blobCountCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(BLOBS_USAGE_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Map<String,Long> load(String __) throws Exception {
                    final var QUERY = "SELECT REGEXP_SUBSTRING(s.blobUrl, '^(\\w+:\\/\\/)?([^\\/]+)\\/([^\\/]+)') AS baseURI, count('*') AS usage From Short s GROUP BY baseURI";
                    var hits = DB.sql(QUERY, JavaShorts.BlobServerCount.class);

                    var candidates = hits.stream().collect( Collectors.toMap( JavaShorts.BlobServerCount::baseURI, JavaShorts.BlobServerCount::count));

                    for( var uri : BlobsClients.all() )
                        candidates.putIfAbsent( uri.toString(), 0L);

                    return candidates;

                }
            });


    private String getLeastLoadedBlobServerURI(String shortId) {
        try {
            var servers = blobCountCache.get(BLOB_COUNT);
            String primary = "";
            String replica = "";

            var leastLoadedServer = servers.entrySet()
                    .stream()
                    .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                    .findFirst();

            if (leastLoadedServer.isPresent()) {
                var uri = leastLoadedServer.get().getKey();
                servers.compute(uri, (k, v) -> v + 1L);
                primary = uri;
            }

            leastLoadedServer = servers.entrySet()
                    .stream()
                    .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                    .findFirst();

            if (leastLoadedServer.isPresent()) {
                var uri = leastLoadedServer.get().getKey();
                servers.compute(uri, (k, v) -> v + 1L);
                replica = uri;
            }

            /*if (primary.equals(replica))
                return primary;
            else if (primary.isEmpty() && replica.isEmpty())
                return "?";
            else if (primary.isEmpty())
                return format("%s/%s/%s", replica, Blobs.NAME, shortId);
            else if (replica.isEmpty())
                return format("%s/%s/%s", primary, Blobs.NAME, shortId);
            else*/
            return format("%s/%s/%s", primary, Blobs.NAME, shortId) + "|" + format("%s/%s/%s", replica, Blobs.NAME, shortId);

        } catch (Exception x) {
            x.printStackTrace();
        }
        return "?";
    }

    static record BlobServerCount(String baseURI, Long count) {};

    private long totalShortsInDatabase() {
        var hits = DB.sql("SELECT count('*') FROM Short", Long.class);
        return 1L + (hits.isEmpty() ? 0L : hits.get(0));
    }

    private String getBlobsUrls(String shortId) {
        var blobsURLs = Discovery.getInstance().knownUrisOf(Blobs.NAME, 1);
        StringBuilder blobUrl = new StringBuilder(format("%s/%s/%s", blobsURLs[0], Blobs.NAME, shortId));

        for (int i = 1; i < blobsURLs.length; i++)
            blobUrl.append("|").append(format("%s/%s/%s", blobsURLs[i], Blobs.NAME, shortId));

        return blobUrl.toString();
    }

    protected Result<Short> shortFromCache(String shortId) {
        try {
            var res = shortsCache.get(shortId);
            var newBlobUrl = getBlobsUrls(shortId);

            if (res.isOK())
                if (!res.value().getBlobUrl().equals(newBlobUrl)) {
                    shortsCache.invalidate(shortId);
                    var shrt = res.value();
                    shrt.setBlobUrl(newBlobUrl);
                    DB.updateOne(shrt);
                    return ok(shrt);
                }

            return shortsCache.get(shortId);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }


}
