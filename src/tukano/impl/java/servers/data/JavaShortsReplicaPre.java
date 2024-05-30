package tukano.impl.java.servers.data;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import tukano.api.User;
import tukano.api.java.Result;

import java.time.Duration;

import static tukano.api.java.Result.ErrorCode.*;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;
import static tukano.impl.java.clients.Clients.UsersClients;

public class JavaShortsReplicaPre {

    private static final long USER_CACHE_EXPIRATION = 3000;

    public Result<User> preVerifyUser(String userId, String password) {
        try {
            return usersCache.get(new Credentials(userId, password));
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    public Result<Void> preVerifyUser( String userId ) {
        var res = preVerifyUser( userId, "");
        if( res.error() == FORBIDDEN )
            return ok();
        else
            return error( res.error() );
    }

    public boolean preVerifyShortId(String shortId) {
        return shortId != null;
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

}
