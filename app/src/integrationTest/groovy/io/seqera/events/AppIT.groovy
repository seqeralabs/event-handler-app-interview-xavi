package io.seqera.events

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals

class AppIT {

    private static def BASE_URL = "http://localhost:8000/events"

    private static def EVENT_JSON = '''{"id":"0","workspaceId":"workspaceId","userId":"userId","cpu":80,"mem":100,"io":4}'''

    @Test
    void 'app can create and retrieve an event'() {
        App.main()

        def postEvent = postEvent()
        assertEquals(200, postEvent.getResponseCode())

        def getEvents = getEvents()
        assertEquals(200, getEvents.getResponseCode())
        assertEquals('[' + EVENT_JSON + ']', getEvents.getInputStream().getText())
    }

    private static HttpURLConnection postEvent() {
        def post = new URL(BASE_URL).openConnection() as HttpURLConnection
        def bytes = EVENT_JSON.getBytes("UTF-8")
        post.setRequestProperty("Content-Type", "application/json")
        post.setRequestMethod("POST")
        post.setDoOutput(true)
        post.getOutputStream().write(bytes)
        return post
    }

    private static HttpURLConnection getEvents() {
        return new URL(BASE_URL).openConnection() as HttpURLConnection
    }
}
