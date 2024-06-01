package tukano.impl.rest.servers;

import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tukano.api.java.Shorts;
import tukano.impl.rest.servers.utils.CustomLoggingFilter;
import tukano.impl.rest.servers.utils.GenericExceptionMapper;
import utils.Args;
import utils.kafka.KafkaUtils;

public class RestShortsReplicaServer extends AbstractRestServer {

    public static final int PORT = 7890;

    private static Logger Log = Logger.getLogger(RestShortsServer.class.getName());

    public RestShortsReplicaServer() {
        super( Log, Shorts.NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.registerInstances( new RestShortsReplicaResource() );
        config.register(new GenericExceptionMapper());
        config.register(new CustomLoggingFilter());
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        Args.use(args);
        KafkaUtils.createTopic("shorts", 1, 1);
        new RestShortsReplicaServer().start();
    }
}
