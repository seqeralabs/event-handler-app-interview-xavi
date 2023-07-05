import com.sun.net.httpserver.HttpServer
import groovy.sql.Sql
import io.seqera.events.dao.EventDao
import io.seqera.events.dao.SqlEventDao
import io.seqera.events.handler.EventHandler
import io.seqera.events.handler.Handler
import io.seqera.events.utils.AppContext
import groovy.yaml.YamlSlurper

class App {

    static PORT = 8000
    static Handler[] handlers
    static HttpServer httpServer
    static AppContext context

    static void main(String[] args) {
        context = buildContext()
        // Building dao
        EventDao dao = new SqlEventDao(context.sql)
        handlers = [new EventHandler(dao)]
        httpServer = startServer()
        /**
         * TODO: implement a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                println 'Stopping server'
                httpServer.stop();
            }
        }) **/
    }


    static AppContext buildContext() {
        def sql = buildDbConnection()
        //TODO: implement a database connection pool mechanism
        //TODO: implement migrations
        return new AppContext(sql: sql)
    }
    static HttpServer startServer() {
        return HttpServer.create(new InetSocketAddress(PORT), /*max backlog*/ 0).with {
            println "Server is listening on ${PORT}, hit Ctrl+C to exit."
            //TODO: implement a dispatching mechanism
            for (def h : handlers){
                createContext(h.handlerPath, h)
            }
            start()
        }
    }

    static migrateFrom(Sql sql, String migrationFolder){
        def folder = new File(App.classLoader.getResource(migrationFolder).toURI())
        def migrationFiles = folder.listFiles  {it -> it.name.endsWith(".sql")}.sort {Long.parseLong(it)} as File[]
        migrationFiles.each {
            sql.execute(it.text)
        }
    }

    static def Sql buildDbConnection() {
        def file = new File(App.class.getResource('/app.yaml').toURI())
        def conf = new YamlSlurper().parse(file)
        def databaseConfig = conf['app']['database']
        def sql = Sql.newInstance(databaseConfig['url'], databaseConfig['username'],databaseConfig['password'], databaseConfig['driver'])
        if(databaseConfig['migrations']) {
            migrateFrom(sql, databaseConfig['migrations'] as String)
        }
        return sql
    }
}
