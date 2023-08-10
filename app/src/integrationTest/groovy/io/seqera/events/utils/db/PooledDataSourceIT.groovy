package io.seqera.events.utils.db

import java.time.Duration
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertTrue

class PooledDataSourceIT {

    @Test
    void 'database connection can be obtained from the pool'() {
        def dataSource = new PooledDataSource(
                'jdbc:hsqldb:mem:events',
                'sa',
                '',
                'org.hsqldb.jdbcDriver',
                Duration.ofSeconds(1),
                3
        )
        assertTrue(dataSource.connection.prepareCall('CALL now()').execute())
    }
}
