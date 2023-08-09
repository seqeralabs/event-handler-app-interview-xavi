package io.seqera.events.utils.db

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.SQLNonTransientConnectionException
import java.sql.SQLTransientConnectionException
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

import static org.awaitility.Awaitility.given
import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertThrows
import static org.mockito.ArgumentMatchers.any
import static org.mockito.ArgumentMatchers.eq
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.never
import static org.mockito.Mockito.verify
import static org.mockito.Mockito.when

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PooledDataSourceTest {

    private Driver driverMock
    private DataSource dataSource
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

    @BeforeEach
    void setUp() {
        dataSource = new PooledDataSource(
                'jdbc:test:events',
                'test',
                '',
                Driver.class.name,
                idleTimeout,
                initialPoolSize
        )
    }

    @AfterEach
    void tearDown() {
        connectionMocks.clear()
    }

    @Test
    void 'database connections are allocated at startup'() {
        // The initial pool size of connections should have been created at startup
        assertEquals(initialPoolSize, connectionMocks.size())
    }

    @Test
    void 'database connections are not physically closed by the application code'() {
        dataSource.connection.close()

        for (connection in connectionMocks) {
            // None of the connection mocks should be closed
            verify(connection, never()).close()
        }
    }

    @Test
    void 'database connections are released when closed by the application code'() {
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
    void 'database connections cannot be reused when closed by the application code'() {
        def connection = dataSource.connection
        connection.close()

        assertThrows(SQLNonTransientConnectionException.class) {
            connection.prepareCall('CALL now()')
        }
    }

    @Test
    void 'database connections are available after closed by the application code'() {
        def connectionMockSize = connectionMocks.size()
        List<Connection> connections = []

        // Allocate all connections in the pool
        for (int i = 0; i < initialPoolSize; i++) {
            connections += dataSource.connection
        }

        // Close all connections in the pool
        connections.forEach { it.close() }

        // New allocated connection without new connections to the database
        assertEquals(connectionMockSize, connectionMocks.size())
        dataSource.connection
    }

    @Test
    void 'database connections are re-established when there is a connection error'() {
        def connectionMockSize = connectionMocks.size()

        // Stub all connections to throw an exception
        for (int i = 0; i < initialPoolSize; i++) {
            when(connectionMocks[i].prepareCall(any()))
                    .thenThrow(new SQLNonTransientConnectionException())
        }

        assertThrows(SQLNonTransientConnectionException.class) {
            dataSource.connection.prepareCall('CALL now()')
        }

        // The new available connection uses a non-stubbed mock
        dataSource.connection.prepareCall('CALL now()')
        assertEquals(connectionMockSize + 1, connectionMocks.size())
    }

    @Test
    void 'database connections are recycled after an idle timeout'() {

        // Allocate all connections in the pool
        for (int i = 0; i < initialPoolSize; i++) {
            dataSource.connection
        }

        // After idle time, connection is available
        given().ignoreException(SQLTransientConnectionException.class)
                .await().atMost(idleTimeout + 1, TimeUnit.SECONDS)
                .until { dataSource.connection != null }
    }

    @Test
    void 'an exception is thrown when no connections are available in the pool'() {

        // Allocate all connections in the pool
        for (int i = 0; i < initialPoolSize; i++) {
            dataSource.connection
        }

        // Next connection request should fail
        assertThrows(SQLTransientConnectionException.class) {
            dataSource.connection
        }
    }
}
