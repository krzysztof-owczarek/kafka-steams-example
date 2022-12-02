package serdes

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets

class JsonSerializer<T> : Serializer<T> {
    private val gson = Gson()
    override fun serialize(topic: String, t: T): ByteArray {
        return gson.toJson(t).toByteArray(StandardCharsets.UTF_8)
    }
}
