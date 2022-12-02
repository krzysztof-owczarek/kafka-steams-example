import domain.Car
import domain.CarManufacturer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import serdes.JsonDeserializer
import serdes.JsonSerializer
import java.util.*


fun main() {

    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "test-app"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9093"
    props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "5000"
    props[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
        LogAndContinueExceptionHandler::class.java

    val serializer = JsonSerializer<Car>()
    val deserializer = JsonDeserializer(Car::class.java)
    val carSerde = Serdes.serdeFrom(serializer, deserializer)

    val manufacturerSerializer = JsonSerializer<CarManufacturer>()
    val manufacturerDeserializer = JsonDeserializer(CarManufacturer::class.java)
    val manufacturerSerde = Serdes.serdeFrom(manufacturerSerializer, manufacturerDeserializer)

    val config = StreamsConfig(props)
    val streamsBuilder = StreamsBuilder()

    // TODO: Produce values
    val storeSupplier = Stores.inMemoryKeyValueStore("test-table-store")
    val manufacturerTable: GlobalKTable<String, CarManufacturer> = streamsBuilder.globalTable(
        "car-manufacturers",
        Consumed.with(Serdes.String(), manufacturerSerde),
        Materialized.`as`<String, CarManufacturer>(storeSupplier)
            .withKeySerde(Serdes.String())
            .withValueSerde(manufacturerSerde)
            .withCachingDisabled()
    )

    // Joining KStream and KTable
    // TODO: Produce values periodically
    val trafficStream = streamsBuilder.stream("input-topic", Consumed.with(Serdes.String(), carSerde))
        .join(manufacturerTable, {_, car -> car.brand.name}, { car, manufacturer -> car.copy(country = manufacturer.countryCode) })

    trafficStream.to("output-topic", Produced.with(Serdes.String(), carSerde))
    trafficStream.print(Printed.toSysOut<String, Car>().withLabel("traffic"))

    // Branching the KStream
    val stringKStream = trafficStream.groupByKey()
        .count(Materialized.`as`<String?, Long?, KeyValueStore<Bytes, ByteArray>?>("counters"))
        .toStream()
        .mapValues { k, v -> "There are $v cars of brand $k in the area right now" }

    // Count cars passed by the street counted by country
    stringKStream.print(Printed.toSysOut<String, String>().withLabel("counterByCountry"))

    // Count german cars passed by the stream
    val germanCars = trafficStream.filter { _, v -> v.country == "DE" }
    germanCars.print(Printed.toSysOut<String, Car>().withLabel("germanCars"))

    val topology = streamsBuilder.build()
    val streams = KafkaStreams(topology, config)

    println("Starting...")
    streams.start()
}


