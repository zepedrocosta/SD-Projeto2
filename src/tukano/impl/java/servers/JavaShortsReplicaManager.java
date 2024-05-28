package tukano.impl.java.servers;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedShorts;
import utils.DB;
import utils.kafka.KafkaPublisher;
import utils.kafka.SyncPoint;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static tukano.api.java.Result.ErrorCode.*;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.errorOrValue;
import static tukano.impl.java.clients.Clients.BlobsClients;
import static tukano.impl.java.clients.Clients.UsersClients;
import static utils.DB.getOne;

public class JavaShortsReplicaManager implements ExtendedShorts {

    private static final long USER_CACHE_EXPIRATION = 3000;
    private static final long SHORTS_CACHE_EXPIRATION = 3000;
    private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;
    private static final String KAFKA_BROKERS = "kafka:9092";
    private static final String TOPIC = "shorts";

    private final KafkaPublisher publisher;
    private final JavaShortsReplicaAction impl;
    final SyncPoint<String> sync;

    public JavaShortsReplicaManager() {
        this.publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        this.sync = new SyncPoint<>();
        this.impl = new JavaShortsReplicaAction();
    }

    @Override
    public Result<Short> createShort(String userId, String password) {

        var precondition = preCreateShort(userId, password);

        //Call JavaShortsPrecondition
        if (precondition.isOK()) {
            var version = publisher.publish(TOPIC, "createShort|" + userId + "|" + password);
        }
        //Publish to Kafka to execute
        //Wait for Sync

        return null;
    }

    public Result<User> preCreateShort(String userId, String password) {
        try {
            return usersCache.get( new JavaShorts.Credentials(userId, password));
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> deleteShort(String shortId, String password) {
        return null;
    }

    @Override
    public Result<Short> getShort(String shortId) {
        return null;
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        return null;
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
        return null;
    }

    @Override
    public Result<List<String>> followers(String userId, String password) {
        return null;
    }

    @Override
    public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
        return null;
    }

    @Override
    public Result<List<String>> likes(String shortId, String password) {
        return null;
    }

    @Override
    public Result<List<String>> getFeed(String userId, String password) {
        return null;
    }

    @Override
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
}
