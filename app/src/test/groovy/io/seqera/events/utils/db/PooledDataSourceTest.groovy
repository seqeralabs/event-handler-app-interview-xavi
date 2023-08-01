package io.seqera.events.utils.db

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

import static org.junit.jupiter.api.Assertions.assertThrows
import static org.mockito.ArgumentMatchers.any
import static org.mockito.ArgumentMatchers.eq
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.never
import static org.mockito.Mockito.times
import static org.mockito.Mockito.verify
import static org.mockito.Mockito.when

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PooledDataSourceTest {

    private Driver driverMock
    private List<Connection> connectionMocks = []

    private int initialPoolSize = 3
    private int idleTimeout = 1

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

    @AfterEach
    void tearDown() {
        connectionMocks.clear()
    }

    @Test
    void 'database connections are allocated at startup'() {
        def dataSource = pooledDataSource()

        // The initial pool size of connections should have been created at startup
        verify(driverMock, times(dataSource.initialPoolSize)).connect(eq("jdbc:test:events"), any())
    }

    @Test
    void 'database connections are not physically closed by the application code'() {
        def dataSource = pooledDataSource()
        dataSource.connection.close()

        for (connection in connectionMocks) {
            // None of the connection mocks should be closed
            verify(connection, never()).close()
        }
    }

    @Test
    void 'database connections are released when closed by the application code'() {
        def dataSource = pooledDataSource()
        List<Connection> connections = []

        // Allocate all connections in the pool
        for (int i = 0; i < initialPoolSize; i++) {
            connections += dataSource.connection
        }

        // Closing a connection should make it available again
        connections.last().close()
        dataSource.connection
    }

    @Test
    void 'database connections are re-established when there is a connection error'() {
        def dataSource = pooledDataSource()

        // Stub all connections to throw an exception
        for (int i = 0; i < initialPoolSize; i++) {
            when(connectionMocks[i].prepareCall(any()))
                    .thenThrow(new SQLNonTransientConnectionException())
        }

        assertThrows(SQLException.class) {
            dataSource.connection.prepareCall("SELECT 1 + 1")
        }

        // The new available connection uses a non-stubbed mock
        dataSource.connection.prepareCall("SELECT 1 + 1")
    }

    @Test
    void 'database connections are recycled after an idle timeout'() {
        def dataSource = pooledDataSource()

        // Allocate all connections in the pool
        for (int i = 0; i < initialPoolSize; i++) {
            dataSource.connection
        }

        // After idle time, connection is available
        Thread.sleep(idleTimeout * 1500)
        dataSource.connection
    }

    @Test
    void 'an exception is thrown when no connections are available in the pool'() {
        def dataSource = pooledDataSource()

        // Allocate all connections in the pool
        for (int i = 0; i < initialPoolSize; i++) {
            dataSource.connection
        }

        // Next connection request should fail
        assertThrows(SQLException.class) {
            dataSource.connection
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
