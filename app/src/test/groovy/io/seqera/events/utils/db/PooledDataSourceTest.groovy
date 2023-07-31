package io.seqera.events.utils.db

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager

import static io.seqera.events.utils.db.PooledDataSource.DEFAULT_IDLE_TIMEOUT
import static io.seqera.events.utils.db.PooledDataSource.DEFAULT_LOGIN_TIMEOUT
import static org.mockito.ArgumentMatchers.any
import static org.mockito.ArgumentMatchers.eq
import static org.mockito.Mockito.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PooledDataSourceTest {

    private Driver driverMock
    private PooledDataSource dataSource

    private def connectionMocks = [
            mock(Connection.class),
            mock(Connection.class),
            mock(Connection.class)
    ]

    @BeforeAll
    void beforeAll() {
        driverMock = mock(Driver.class)

        // We need to stub the driver to allow DriverManager check the connection
        when(driverMock.acceptsURL(eq("jdbc:test:events"))).thenReturn(true)

        def connectionMocksCopy = connectionMocks.clone()
        when(driverMock.connect(eq("jdbc:test:events"), any())).thenAnswer {
            // Return new connection mock per call
            connectionMocksCopy.remove(0)
        }

        DriverManager.registerDriver(driverMock, {})

        dataSource = new PooledDataSource(
                "jdbc:test:events",
                "test",
                "",
                Driver.class.name,
                DEFAULT_LOGIN_TIMEOUT,
                DEFAULT_IDLE_TIMEOUT,
                3,

        )
    }

    @Test
    void 'database connections are allocated at startup'() {
        dataSource.getConnection()

        verify(driverMock, times(dataSource.initialPoolSize))
                .connect(eq("jdbc:test:events"), any())
    }

    @Test
    void 'database connections are not closed by the application code'() {
        dataSource.getConnection().close()

        for (connectionMock in connectionMocks) {
            verify(connectionMock, never()).close()
        }
    }
}
