package io.seqera.events.utils.db

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.SQLException

import static org.junit.jupiter.api.Assertions.assertThrows
import static org.mockito.ArgumentMatchers.any
import static org.mockito.ArgumentMatchers.eq
import static org.mockito.Mockito.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PooledDataSourceTest {

    private Driver driverMock

    private int initialPoolSize = 3
    private int idleTimeout = 100

    private List<Connection> connectionMocks = []

    @BeforeAll
    void beforeAll() {
        driverMock = mock(Driver.class)

        // We need to stub the driver to allow DriverManager check the connection
        when(driverMock.acceptsURL(eq("jdbc:test:events"))).thenReturn(true)
        when(driverMock.connect(eq("jdbc:test:events"), any())).thenAnswer {
            // Return new connection mock per call
            connectionMocks += mock(Connection.class)
            connectionMocks.last()
        }

        DriverManager.registerDriver(driverMock, {})
    }

    @Test
    void 'database connections are allocated at startup'() {
        def dataSource = pooledDataSource()
        verify(driverMock, times(dataSource.initialPoolSize)).connect(eq("jdbc:test:events"), any())
    }

    @Test
    void 'database connections are not closed by the application code'() {
        def dataSource = pooledDataSource()
        dataSource.getConnection().close()

        for (connection in connectionMocks) {
            verify(connection, never()).close()
        }
    }

    @Test
    void 'an exception is thrown when no connections are available in the pool'() {
        def dataSource = pooledDataSource()
        for (int i = 0; i < initialPoolSize; i++) {
            dataSource.getConnection()
        }
        assertThrows(SQLException.class) {
            dataSource.getConnection()
        }
    }

    private PooledDataSource pooledDataSource() {
        new PooledDataSource("jdbc:test:events",
                "test",
                "",
                Driver.class.name,
                idleTimeout,
                initialPoolSize
        )
    }
}
