import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import java.util.*

fun main(args: Array<String>) {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "test-app"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9093"

    val config = StreamsConfig(props)
    val streamsBuilder = StreamsBuilder()

    streamsBuilder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues { x -> x.uppercase() }
        .peek { x, y -> println("x=$x, y=$y") }
        .to("output-topic", Produced.with(Serdes.String(), Serdes.String()))

    val streams = KafkaStreams(streamsBuilder.build(), config)

    println("Starting...")
    streams.start()
//    TimeUnit.SECONDS.sleep(2L)
//    println("Shutting down!")
//    streams.close()
}