import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.nio.IOUtil;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;
import java.io.File;

import static com.hazelcast.examples.helper.LicenseUtils.ENTERPRISE_LICENSE_KEY;

/**
 * You have to set your Hazelcast Enterprise license key to make this code sample work.
 * Please have a look at {@link com.hazelcast.examples.helper.LicenseUtils} for details.
 */
public class JCacheHotRestart_Dump {

    private static final String HOT_RESTART_ROOT_DIR = System.getProperty("java.io.tmpdir")
            + File.separatorChar + "hazelcast-hot-restart-dump";

    private static final String BACK_UP_DIR = System.getProperty("java.io.tmpdir")
            + File.separatorChar + "hazelcast-hot-restart-backup";

    private static final String HOT_RESTART_LOAD_ROOT_DIR = System.getProperty("java.io.tmpdir")
            + File.separatorChar + "hazelcast-hot-restart-load";


    public static void main(String[] args) {
        IOUtil.delete(new File(HOT_RESTART_ROOT_DIR));
        IOUtil.delete(new File(BACK_UP_DIR));
        IOUtil.delete(new File(HOT_RESTART_LOAD_ROOT_DIR));
        // create the load dir . Manually move the contents of Base Dir / backup dir to the load dir before
        // starting the load process
        new File(HOT_RESTART_LOAD_ROOT_DIR).mkdir();

        Config config = new Config();
        config.setLicenseKey(ENTERPRISE_LICENSE_KEY);

        config.getNetworkConfig().setPort(5701).setPortAutoIncrement(false);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        HotRestartPersistenceConfig hotRestartConfig = config.getHotRestartPersistenceConfig();
        hotRestartConfig.setEnabled(true).setBaseDir(new File(HOT_RESTART_ROOT_DIR));

        hotRestartConfig.setParallelism(1);
        hotRestartConfig.setValidationTimeoutSeconds(120);
        hotRestartConfig.setDataLoadTimeoutSeconds(900);
        hotRestartConfig.setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
        hotRestartConfig.setBackupDir(new File(BACK_UP_DIR));
        config.setHotRestartPersistenceConfig(hotRestartConfig);

        HazelcastInstance instance = null;
        Cache<Integer, String> cache =null;
        //populate cache and backup
        instance = Hazelcast.newHazelcastInstance(config);
        cache = createCache(instance);
        for (int i = 0; i < 10; i++) {
            cache.put(i, "value" + i);
        }

        HotRestartService service = instance.getCluster().getHotRestartService();
        service.backup();

        // HotRestartStateImpl status = service.getBackupTaskStatus();
        instance.shutdown();

        // verify
        instance = Hazelcast.newHazelcastInstance(config);
        cache = createCache(instance);

        for (int i = 0; i < 10; i++) {
            System.out.println("cache.get(" + i + ") = " + cache.get(i));
        }

        Hazelcast.shutdownAll();
    }

    private static Cache<Integer, String> createCache(HazelcastInstance instance) {
        CachingProvider cachingProvider = HazelcastServerCachingProvider
            .createCachingProvider(instance);

        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>("cache");
        cacheConfig.getHotRestartConfig().setEnabled(true);

        return cachingProvider.getCacheManager().createCache("cache", cacheConfig);
    }
}
