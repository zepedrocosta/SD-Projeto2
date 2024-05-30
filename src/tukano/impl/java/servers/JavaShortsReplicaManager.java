package tukano.impl.java.servers;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import utils.DB;
import utils.kafka.KafkaPublisher;
import utils.kafka.KafkaSubscriber;
import utils.kafka.RecordProcessor;
import utils.kafka.SyncPoint;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static tukano.api.java.Result.*;
import static tukano.api.java.Result.ErrorCode.*;
import static tukano.impl.java.clients.Clients.BlobsClients;
import static tukano.impl.java.clients.Clients.UsersClients;
import static utils.DB.getOne;

public class JavaShortsReplicaManager implements ExtendedShorts, RecordProcessor {

    private static final long USER_CACHE_EXPIRATION = 3000;
    private static final long SHORTS_CACHE_EXPIRATION = 3000;
    private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;
    private static final String BLOB_COUNT = "*";
    private static final String KAFKA_BROKERS = "kafka:9092";
    private static final String TOPIC = "shorts";

    private final KafkaPublisher publisher;
    private final KafkaSubscriber receiver;
    private final JavaShortsReplicaAction impl;
    private final ObjectMapper mapper;

    AtomicLong counter = new AtomicLong(totalShortsInDatabase());
    final SyncPoint<String> sync;

    public JavaShortsReplicaManager() {
        this.publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), "earliest");
        this.sync = SyncPoint.getInstance();
        this.impl = new JavaShortsReplicaAction();
        this.mapper = new ObjectMapper();
        receiver.start(false, this);
    }

    @Override
    public void onReceive(ConsumerRecord<String, String> r) {
        String msg = r.value();
        String[] parts = msg.split("\\$");
        String operation = parts[0];
        Result<String> result = null;
        try {
            switch (operation) {
                case "create" -> result = impl.createShort(mapper.readValue(parts[1], Short.class));
                case "getShort" -> result = impl.getShort(parts[1]);
                /*case "getShorts" -> result = impl.getShorts(parts[1]);
                case "deleteShort" -> result = impl.deleteShort(parts[1], parts[2]);
                case "follow" -> result = impl.follow(parts[1], parts[2], Boolean.parseBoolean(parts[3]), parts[4]);
                case "followers" -> result = impl.followers(parts[1], parts[2]);
                case "like" -> result = impl.like(parts[1], parts[2], Boolean.parseBoolean(parts[3]), parts[4]);
                case "likes" -> result = impl.likes(parts[1], parts[2]);
                case "getFeed" -> result = impl.getFeed(parts[1], parts[2]);
                case "deleteAllShorts" -> result = impl.deleteAllShorts(parts[1], parts[2], parts[3]);*/
            }

            var version = r.offset();

            if (result.isOK())
                sync.setResult(version, result.value());
            else
                sync.setResult(version, mapper.writeValueAsString(result.error()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Result<Short> createShort(String userId, String password) {

        var precondition = preCreateShort(userId, password);
        var shortId = format("%s-%d", userId, counter.incrementAndGet());
        var shrt = new Short(shortId, userId, getLeastLoadedBlobServerURI(shortId));

        if (precondition.isOK()) {
            try {
                var version = publisher.publish(TOPIC, "create$" + mapper.writeValueAsString(shrt));
                var result = sync.waitForResult(version);
                shrt = mapper.readValue(result, Short.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        return ok(shrt);
    }

    private Result<User> preCreateShort(String userId, String password) {
        try {
            return usersCache.get(new JavaShorts.Credentials(userId, password));
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Short> getShort(String shortId) {
        var precondition = preGetShort(shortId);
        Short shrt = null;

        if (precondition) {
            var version = publisher.publish(TOPIC, "getShort$" + shortId);
            var result = sync.waitForResult(version);
            try {
                shrt = mapper.readValue(result, Short.class);
            } catch (JsonProcessingException e) {
                try {
                    return error(mapper.readValue(result, ErrorCode.class));
                } catch(JsonProcessingException ex) {
                    ex.printStackTrace();
                }
            }
        }

        return ok(shrt);
    }

    private boolean preGetShort(String shortId) {
        return shortId != null;
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        return null;
    }

    @Override
    public Result<Void> deleteShort(String shortId, String password) {
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

    private long totalShortsInDatabase() {
        var hits = DB.sql("SELECT count('*') FROM Short", Long.class);
        return 1L + (hits.isEmpty() ? 0L : hits.get(0));
    }

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

}
