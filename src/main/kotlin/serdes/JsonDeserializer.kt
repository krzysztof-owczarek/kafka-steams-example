package serdes

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Deserializer

class JsonDeserializer<T>(private val deserializedClass: Class<T>) : Deserializer<T> {
    private val gson = Gson()
    override fun deserialize(topic: String, bytes: ByteArray?): T? {
        if (bytes == null) return null
        return gson.fromJson(String(bytes), deserializedClass)
    }
}
