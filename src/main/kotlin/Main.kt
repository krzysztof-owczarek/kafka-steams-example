import com.google.gson.Gson
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.kstream.Produced
import java.nio.charset.StandardCharsets
import java.util.*


fun main(args: Array<String>) {

    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "test-app"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9093"
    props[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
        LogAndContinueExceptionHandler::class.java

    val serializer = JsonSerializer<MyContainer>()
    val deserializer = JsonDeserializer(MyContainer::class.java)
    val serde = Serdes.serdeFrom(serializer, deserializer)

    val config = StreamsConfig(props)
    val streamsBuilder = StreamsBuilder()

    val upperStream = streamsBuilder.stream("input-topic", Consumed.with(Serdes.String(), serde))
//    val upperStream = streamsBuilder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues { x -> x.value.uppercase() }
//        .mapValues { x -> x.uppercase() }
//        .print(Printed.toSysOut<String, String>().withLabel("my-container"))
//        .mapValues { x -> x + " _prefix" }
//        .to("output-topic", Produced.with(serde, serde))
//        .to("output-topic", Produced.with(Serdes.String(), Serdes.String()))

    upperStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()))
    upperStream.print(Printed.toSysOut<String, String>().withLabel("my-container"))

    val streams = KafkaStreams(streamsBuilder.build(), config)

    println("Starting...")
    streams.start()
}

class JsonSerializer<T> : Serializer<T> {
    private val gson = Gson()
    override fun serialize(topic: String, t: T): ByteArray {
        return gson.toJson(t).toByteArray(StandardCharsets.UTF_8)
    }
}

class JsonDeserializer<T>(private val deserializedClass: Class<T>) : Deserializer<T> {
    private val gson = Gson()
    override fun deserialize(topic: String, bytes: ByteArray?): T? {
        if (bytes == null) return null
        return gson.fromJson(String(bytes), deserializedClass)
    }

}
