// simple simulation for neocortix cloud services
package neocortix

import scala.concurrent.duration._

import io.gatling.core.Predef._
import io.gatling.http.Predef._

class ncsSim extends Simulation {

  val httpProtocol = http
    .baseUrl("https://loadtest-target.neocortix.com")
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8") // 6
    .doNotTrackHeader("1")
    .acceptLanguageHeader("en-US,en;q=0.5")
    .acceptEncodingHeader("gzip, deflate")
    .userAgentHeader("Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0")
    .shareConnections

  val scn = scenario("scenario_1")
    .exec(http("request_1")
      .get("/"))
    .pause( 100.milliseconds )
    .exec(http("request_2")
      .get("/"))
    .pause( 1000.milliseconds )
    .exec(http("request_3")
      .get("/"))
    .pause( 1000.milliseconds )
    .exec(http("request_4")
      .get("/"))
    .pause( 1000.milliseconds )
    .exec(http("request_5")
      .get("/"))
    .pause( 1000.milliseconds )

  setUp( // 11
    //scn.inject(atOnceUsers(1)) // just one user
    scn.inject( constantConcurrentUsers( 6 ) during (90 seconds ) )
    //scn.inject( rampConcurrentUsers(1) to (6) during (45 seconds), constantConcurrentUsers( 6 ) during (45 seconds ) )
    //scn.inject( rampConcurrentUsers(1) to (6) during (360 seconds), constantConcurrentUsers( 6 ) during (30 seconds ) )
  ).protocols(httpProtocol) // 13
}
