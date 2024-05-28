package tukano.impl.java.servers;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Blobs;
import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedShorts;
import utils.DB;
import utils.kafka.KafkaSubscriber;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static tukano.api.java.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.java.Result.ErrorCode.TIMEOUT;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.errorOrValue;
import static tukano.impl.java.clients.Clients.BlobsClients;
import static tukano.impl.java.clients.Clients.UsersClients;
import static utils.DB.getOne;

public class JavaShortsReplicaAction implements ExtendedShorts {

    static final String TOPIC = "shorts";
    static final String KAFKA_BROKERS = "kafka:9092";
    private static final long USER_CACHE_EXPIRATION = 3000;
    private static final long SHORTS_CACHE_EXPIRATION = 3000;
    private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;
    private static final String BLOB_COUNT = "*";

    AtomicLong counter = new AtomicLong( totalShortsInDatabase() );
    private final KafkaSubscriber receiver;
    private static Logger Log = Logger.getLogger(JavaShortsReplicaAction.class.getName());

    public JavaShortsReplicaAction() {
        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), "earliest");
        receiver.start(false, this::operation);
    }

    public void operation(ConsumerRecord<String, String> record) {
        String msg = record.value();
        String[] parts = msg.split("\\|");
        String operation = parts[0];
        switch (operation) {
            case "createShort":
                createShort(parts[1], parts[2]);
                break;
            case "deleteShort":
                deleteShort(parts[1], parts[2]);
                break;
            case "getShort":
                getShort(parts[1]);
                break;
            case "getShorts":
                getShorts(parts[1]);
                break;
            case "follow":
                follow(parts[1], parts[2], Boolean.parseBoolean(parts[3]), parts[4]);
                break;
            case "followers":
                followers(parts[1], parts[2]);
                break;
            case "like":
                like(parts[1], parts[2], Boolean.parseBoolean(parts[3]), parts[4]);
                break;
            case "likes":
                likes(parts[1], parts[2]);
                break;
            case "getFeed":
                getFeed(parts[1], parts[2]);
                break;
            case "deleteAllShorts":
                deleteAllShorts(parts[1], parts[2], parts[3]);
                break;
        }
    }

    @Override
    public Result<Short> createShort(String userId, String password) {
        Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));
        var shortId = format("%s-%d", userId, counter.incrementAndGet());
        var shrt = new Short(shortId, userId, getLeastLoadedBlobServerURI(shortId));

        return DB.insertOne(shrt);
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


}
