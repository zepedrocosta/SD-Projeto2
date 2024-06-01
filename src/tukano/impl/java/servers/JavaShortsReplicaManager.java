package tukano.impl.java.servers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tukano.api.Short;
import tukano.api.java.Blobs;
import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedShorts;
import utils.DB;
import utils.Token;
import utils.kafka.KafkaPublisher;
import utils.kafka.KafkaSubscriber;
import utils.kafka.RecordProcessor;
import utils.kafka.SyncPoint;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static tukano.api.java.Result.*;
import static tukano.api.java.Result.ErrorCode.FORBIDDEN;
import static tukano.impl.java.clients.Clients.BlobsClients;

public class JavaShortsReplicaManager implements ExtendedShorts, RecordProcessor {

    private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;
    private static final String BLOB_COUNT = "*";
    private static final String KAFKA_BROKERS = "kafka:9092";
    private static final String TOPIC = "shorts";
    private static final String BREAK = "\\{\\$br}";

    private static Logger Log = Logger.getLogger(JavaShortsReplicaManager.class.getName());
    AtomicLong counter = new AtomicLong( totalShortsInDatabase() );
    private final KafkaPublisher publisher;
    private final KafkaSubscriber receiver;
    private final JavaShortsReplicaAction implAction;
    private final JavaShortsReplicaPre implPre;
    private final ObjectMapper mapper;
    final SyncPoint<String> sync;

    public JavaShortsReplicaManager() {
        this.publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), "earliest");
        this.sync = SyncPoint.getInstance();
        this.implAction = new JavaShortsReplicaAction();
        this.implPre = new JavaShortsReplicaPre();
        this.mapper = new ObjectMapper();
        receiver.start(false, this);
    }

    @Override
    public void onReceive(ConsumerRecord<String, String> r) {
        String msg = r.value();
        String[] parts = msg.split(BREAK);
        String operation = parts[0];
        Result<String> result = ok("");
        try {
            switch (operation) {
                case "createShort" -> result = implAction.createShort(mapper.readValue(parts[1], Short.class));
                case "getShort" -> result = implAction.getShort(parts[1]);
                case "deleteShort" -> result = implAction.deleteShort(mapper.readValue(parts[1], Short.class));
                case "getShorts" -> result = implAction.getShorts(parts[1]);
                case "follow" -> result = implAction.follow(parts[1], parts[2], Boolean.parseBoolean(parts[3]));
                case "followers" -> result = implAction.followers(parts[1]);
                case "like" -> result = implAction.like(parts[1], parts[2], Boolean.parseBoolean(parts[3]), parts[4]);
                case "likes" -> result = implAction.likes(parts[1]);
                case "getFeed" -> result = implAction.getFeed(parts[1]);
                case "deleteAllShorts" -> result = implAction.deleteAllShorts(parts[1], parts[2]);
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
        Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

        var precondition = implPre.preVerifyUser(userId, password);
        var shortId = format("%s-%d", userId, counter.incrementAndGet());
        var shrt = new Short(shortId, userId, getLeastLoadedBlobServerURI(shortId));
        String result = "";

        if (!precondition.isOK())
            return error(precondition.error());

        try {
            var version = publisher.publish(TOPIC, "createShort" + BREAK + mapper.writeValueAsString(shrt));
            result = sync.waitForResult(version);
            return ok(mapper.readValue(result, Short.class));
        } catch (JsonProcessingException e) {
            try {
                return error(mapper.readValue(result, ErrorCode.class));
            } catch (JsonProcessingException ex) {
                ex.printStackTrace();
            }
        }

        return error(ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Short> getShort(String shortId) {
        Log.info(() -> format("getShort : shortId = %s\n", shortId));

        var precondition = implPre.preVerifyShortId(shortId);

        if (precondition) {
            var version = publisher.publish(TOPIC, "getShort" + BREAK + shortId);
            var result = sync.waitForResult(version);
            try {
                var shrt = mapper.readValue(result, Short.class);
                return ok(shrt);
            } catch (JsonProcessingException e) {
                try {
                    return error(mapper.readValue(result, ErrorCode.class));
                } catch (JsonProcessingException ex) {
                    ex.printStackTrace();
                }
            }
        }

        return error(ErrorCode.BAD_REQUEST);
    }

    @Override
    public Result<Void> deleteShort(String shortId, String password) {
        Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));

        var res = getShort(shortId);
        if (!res.isOK())
            return error(res.error());

        var shrt = res.value();

        var preconditionUser = implPre.preVerifyUser(shrt.getOwnerId(), password);
        if (!preconditionUser.isOK())
            return error(preconditionUser.error());

        try {
            var version = publisher.publish(TOPIC, "deleteShort" + BREAK + mapper.writeValueAsString(shrt));
            var result = sync.waitForResult(version);
            try {
                return error(mapper.readValue(result, ErrorCode.class));
            } catch (Exception ignored) {
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return ok();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Result<List<String>> getShorts(String userId) {
        Log.info(() -> format("getShorts : userId = %s\n", userId));

        var preconditionUser = implPre.preVerifyUser(userId);
        if (!preconditionUser.isOK())
            return error(preconditionUser.error());

        var version = publisher.publish(TOPIC, "getShorts" + BREAK + userId);
        var result = sync.waitForResult(version);
        try {
            return ok(mapper.readValue(result, List.class));
        } catch (JsonProcessingException e) {
            try {
                return error(mapper.readValue(result, ErrorCode.class));
            } catch (JsonProcessingException ex) {
                ex.printStackTrace();
            }
        }

        return error(ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
        Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));


        var preconditionUser1 = implPre.preVerifyUser(userId1, password);
        if (!preconditionUser1.isOK())
            return error(preconditionUser1.error());

        var preconditionUser2 = implPre.preVerifyUser(userId2);
        if (!preconditionUser2.isOK())
            return error(preconditionUser2.error());

        var version = publisher.publish(TOPIC, "follow" + BREAK + userId1 +  BREAK + userId2  + BREAK + isFollowing);
        var result = sync.waitForResult(version);

        try {
            return error(mapper.readValue(result, ErrorCode.class));
        } catch (Exception ignored) {
        }

        return ok();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Result<List<String>> followers(String userId, String password) {
        Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

        var preconditionUser = implPre.preVerifyUser(userId, password);
        if (!preconditionUser.isOK())
            return error(preconditionUser.error());

        var version = publisher.publish(TOPIC, "followers" + BREAK + userId  + BREAK + password);
        var result = sync.waitForResult(version);
        try {
            return ok(mapper.readValue(result, List.class));
        } catch (JsonProcessingException e) {
            try {
                return error(mapper.readValue(result, ErrorCode.class));
            } catch (JsonProcessingException ex) {
                ex.printStackTrace();
            }
        }

        return error(ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
        Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

        var preconditionUser = implPre.preVerifyUser(userId, password);
        if (!preconditionUser.isOK())
            return error(preconditionUser.error());

        var res = getShort(shortId);
        if (!res.isOK())
            return error(res.error());

        var version = publisher.publish(TOPIC, "like" + BREAK + shortId  + BREAK + userId  + BREAK + isLiked  + BREAK + res.value().getOwnerId());
        var result = sync.waitForResult(version);

        try {
            return error(mapper.readValue(result, ErrorCode.class));
        } catch (Exception ignored) {
        }

        return ok();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Result<List<String>> likes(String shortId, String password) {
        Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

        var res = getShort(shortId);
        if (!res.isOK())
            return error(res.error());

        var shrt = res.value();

        var preconditionUser = implPre.preVerifyUser(shrt.getOwnerId(), password);
        if (!preconditionUser.isOK())
            return error(preconditionUser.error());

        var version = publisher.publish(TOPIC, "likes" + BREAK + shortId);
        var result = sync.waitForResult(version);
        try {
            return ok(mapper.readValue(result, List.class));
        } catch (JsonProcessingException e) {
            try {
                return error(mapper.readValue(result, ErrorCode.class));
            } catch (JsonProcessingException ex) {
                ex.printStackTrace();
            }
        }

        return error(ErrorCode.INTERNAL_ERROR);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Result<List<String>> getFeed(String userId, String password) {
        Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

        var preconditionUser = implPre.preVerifyUser(userId, password);
        if (!preconditionUser.isOK())
            return error(preconditionUser.error());

        var version = publisher.publish(TOPIC, "getFeed" + BREAK + userId);
        var result = sync.waitForResult(version);
        try {
            return ok(mapper.readValue(result, List.class));
        } catch (JsonProcessingException e) {
            try {
                return error(mapper.readValue(result, ErrorCode.class));
            } catch (JsonProcessingException ex) {
                ex.printStackTrace();
            }
        }

        return error(ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Void> deleteAllShorts(String userId, String password, String token) {
        Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

        if( !Token.matches( token ) )
            return error(FORBIDDEN);

        var version = publisher.publish(TOPIC, "deleteAllShorts" + BREAK + userId  + BREAK + password);
        var result = sync.waitForResult(version);

        try {
            return error(mapper.readValue(result, ErrorCode.class));
        } catch (Exception ignored) {
        }

        return ok();
    }

    static record BlobServerCount(String baseURI, Long count) {
    }

    ;

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

            return format("%s/%s/%s", primary, Blobs.NAME, shortId) + "|" + format("%s/%s/%s", replica, Blobs.NAME, shortId);

        } catch (Exception x) {
            x.printStackTrace();
        }
        return "?";
    }

    private long totalShortsInDatabase() {
        var hits = DB.sql("SELECT count('*') FROM Short", Long.class);
        return 1L + (hits.isEmpty() ? 0L : hits.get(0));
    }

}
