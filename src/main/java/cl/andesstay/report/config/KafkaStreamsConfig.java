package cl.andesstay.report.config;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * El cliente de Kafka Streams de este servicio.
 *
 * EL application.id ES EL GRUPO. Kafka Streams lo usa como group.id del consumidor y como prefijo
 * de los temas internos que pueda necesitar. Levantar varias instancias con el mismo application.id
 * reparte las particiones entre ellas, exactamente igual que un consumer group normal, y el reparto
 * se ve en Kafka UI como un grupo mas.
 *
 * TRES HILOS porque el tema tiene tres particiones: una instancia sola puede ocuparlas todas, y si
 * se levantan tres instancias cada una se queda con una. Es el mismo numero por el mismo motivo.
 *
 * LOS ERRORES DE DESERIALIZACION no detienen la topologia. Con el manejador por defecto, un solo
 * mensaje con formato invalido mata el hilo y la particion deja de avanzar: un mensaje envenenado
 * bastaria para congelar los KPIs. LogAndContinue lo registra y sigue, que para un panel es la
 * eleccion correcta —perder un punto de la serie es malo, perder la serie entera es peor—. De los
 * mensajes que fallan al procesarse se ocupa aparte el consumidor, con su reintento y su cola de
 * descarte.
 *
 * EL ESTADO SE RESPALDA SOLO. Cada almacen tiene su changelog en el clúster, con tres replicas:
 * si la instancia se pierde, el estado se reconstruye desde ahi en vez de releer el tema entero.
 *
 * LA CONDICION. Toda la clase cuelga de la bandera del clúster. Sin ella, Streams arrancaria
 * buscando brokers que no existen en los entornos que todavia corren sin Kafka.
 */
@Configuration
@EnableKafkaStreams
@ConditionalOnProperty(name = "andesstay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${andesstay.kafka.streams.application-id}")
    private String applicationId;

    @Value("${andesstay.kafka.streams.state-dir}")
    private String directorioDeEstado;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration configuracionDeStreams() {
        Map<String, Object> propiedades = new HashMap<>();

        propiedades.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        propiedades.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propiedades.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        propiedades.put(StreamsConfig.STATE_DIR_CONFIG, directorioDeEstado);

        // Lo que escribe la topologia hereda las mismas garantias que el productor de reservas.
        propiedades.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        propiedades.put(StreamsConfig.producerPrefix("acks"), "all");
        propiedades.put(StreamsConfig.producerPrefix("compression.type"), "snappy");
        propiedades.put(StreamsConfig.producerPrefix("linger.ms"), 20);

        propiedades.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        return new KafkaStreamsConfiguration(propiedades);
    }
}
