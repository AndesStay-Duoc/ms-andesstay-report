package cl.andesstay.report.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Que pasa cuando un mensaje no se puede procesar.
 *
 * EL PROBLEMA QUE RESUELVE. Antes no habia manejador: el consumidor envolvia todo en un try y, al
 * fallar, registraba el error y NO confirmaba el offset. Eso no es reintentar, es bloquear: la
 * particion vuelve a entregar el mismo mensaje indefinidamente y los que vienen detras no llegan
 * nunca. Un solo registro imposible de procesar paraba la auditoria entera de esa particion.
 *
 * LA ESCALERA. Tres intentos con espera creciente de 1 s, 2 s y 4 s. Es la misma forma que ya usa
 * el flujo de RabbitMQ en ms-andesstay-notify, y por el mismo motivo: un fallo transitorio —la
 * base reiniciandose, un bloqueo momentaneo— se arregla solo si se le da un respiro, y esperar
 * cada vez mas evita castigar a un sistema que ya esta en apuros.
 *
 * LO QUE NO SE REINTENTA. Un mensaje mal formado o sin los campos obligatorios no va a mejorar por
 * esperar: reintentarlo solo gasta tiempo y ruido. Esos fallos se clasifican como irrecuperables y
 * van al tema de descarte en el primer intento.
 *
 * EL TEMA DE DESCARTE es el del tema original mas el sufijo .DLT, con la misma particion. Al
 * mensaje se le añaden las cabeceras estandar de Spring Kafka con el tema, la particion y el
 * offset de origen, la excepcion y el momento del fallo: es lo que permite entender despues por
 * que se descarto sin tener que adivinarlo.
 *
 * Y EL OFFSET SE CONFIRMA. Una vez el mensaje esta a salvo en el tema de descarte, la particion
 * sigue avanzando. Un mensaje envenenado nunca detiene el consumo.
 */
@Slf4j
@Configuration
public class KafkaErrorConfig {

    private static final String SUFIJO_DE_DESCARTE = ".DLT";
    private static final long ESPERA_INICIAL_MS = 1_000L;
    private static final double MULTIPLICADOR = 2.0;
    private static final long ESPERA_MAXIMA_MS = 4_000L;
    private static final int INTENTOS = 3;

    /**
     * Los fallos que no mejoran por esperar. Una sola lista para dos usos: decirle al manejador que
     * no los reintente y decidir que dice el log. Si fueran dos listas, el log podria afirmar
     * "tras 3 intentos" de algo que no se reintento nunca, que es justo lo que pasaba antes.
     */
    private static final List<Class<? extends Exception>> IRRECUPERABLES = List.of(
            DeserializationException.class,
            IllegalArgumentException.class,
            ConversionException.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * El productor del tema de descarte escribe lo que haya que escribir: bytes u objetos.
     *
     * POR QUE NO SOLO BYTES. Antes el valor se serializaba siempre con ByteArraySerializer. Servia
     * para el JSON mal formado, que llega aqui como los bytes originales, pero no para un JSON
     * valido que falla despues: un hecho sin eventId o una base caida tras los reintentos. En ese
     * punto el valor ya es un ReservationEvent, ByteArraySerializer lanza SerializationException, la
     * publicacion en la DLT falla y el contenedor vuelve a entregar el mismo registro sin fin. Un
     * solo mensaje dejaba la particion bloqueada.
     *
     * POR QUE DELEGAR POR TIPO. byte[] se reenvia tal cual, para conservar intacta la evidencia de
     * lo que no se pudo leer. Cualquier otro objeto se escribe como JSON, sin cabeceras de tipo
     * (ningun consumidor de la DLT deberia depender de una clase Java de este servicio) y con las
     * fechas en ISO 8601, que es lo que fija el contrato del envelope: asi lo que queda en la DLT se
     * lee igual que el original. El orden del mapa importa, porque se evalua de arriba abajo.
     *
     * LA CLAVE tambien delega: casi siempre es el reservationId como String, pero si lo que fallo
     * fue deserializar la clave, llega como byte[].
     */
    @Bean
    public KafkaTemplate<Object, Object> descarteKafkaTemplate() {
        Map<String, Object> propiedades = new HashMap<>();
        propiedades.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propiedades.put(ProducerConfig.ACKS_CONFIG, "all");
        propiedades.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);

        Map<Class<?>, Serializer<?>> clavePorTipo = new LinkedHashMap<>();
        clavePorTipo.put(byte[].class, new ByteArraySerializer());
        clavePorTipo.put(String.class, new StringSerializer());

        ObjectMapper mapeador = JacksonUtils.enhancedObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        Map<Class<?>, Serializer<?>> valorPorTipo = new LinkedHashMap<>();
        valorPorTipo.put(byte[].class, new ByteArraySerializer());
        valorPorTipo.put(Object.class, new JsonSerializer<>(mapeador).noTypeInfo());

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(propiedades,
                new DelegatingByTypeSerializer(clavePorTipo),
                new DelegatingByTypeSerializer(valorPorTipo, true)));
    }

    @Bean
    public CommonErrorHandler manejadorDeErrores(KafkaTemplate<Object, Object> descarteKafkaTemplate) {

        DeadLetterPublishingRecoverer descarte = new DeadLetterPublishingRecoverer(
                descarteKafkaTemplate,
                (registro, excepcion) -> {
                    // La causa es la mas especifica de la cadena: el mensaje de la excepcion
                    // exterior es "Listener method ... threw exception", que no dice nada.
                    Throwable causa = NestedExceptionUtils.getMostSpecificCause(excepcion);
                    if (esIrrecuperable(excepcion)) {
                        log.error("[Report] Mensaje descartado sin reintentar (fallo irrecuperable): "
                                        + "topic={} particion={} offset={} causa={}: {}",
                                registro.topic(), registro.partition(), registro.offset(),
                                causa.getClass().getSimpleName(), causa.getMessage());
                    } else {
                        log.error("[Report] Mensaje descartado tras {} intentos: "
                                        + "topic={} particion={} offset={} causa={}: {}",
                                INTENTOS, registro.topic(), registro.partition(), registro.offset(),
                                causa.getClass().getSimpleName(), causa.getMessage());
                    }
                    return new TopicPartition(registro.topic() + SUFIJO_DE_DESCARTE, registro.partition());
                });

        ExponentialBackOff escalera = new ExponentialBackOff(ESPERA_INICIAL_MS, MULTIPLICADOR);
        escalera.setMaxInterval(ESPERA_MAXIMA_MS);
        escalera.setMaxAttempts(INTENTOS - 1);

        DefaultErrorHandler manejador = new DefaultErrorHandler(descarte, escalera);

        // Sin vuelta de hoja: un mensaje con formato invalido o sin los campos obligatorios no
        // mejora por esperar. Va al tema de descarte en el primer intento.
        @SuppressWarnings("unchecked")
        Class<? extends Exception>[] irrecuperables = IRRECUPERABLES.toArray(new Class[0]);
        manejador.addNotRetryableExceptions(irrecuperables);

        manejador.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);
        return manejador;
    }

    /**
     * Si el fallo es de los que no se reintentan, mirando toda la cadena de causas.
     *
     * Hay que recorrerla porque el contenedor envuelve lo que lanza el listener en una
     * ListenerExecutionFailedException: la IllegalArgumentException de IngestaDeEventosService llega como
     * causa, no como excepcion exterior. El manejador de Spring la clasifica del mismo modo.
     */
    private static boolean esIrrecuperable(Throwable excepcion) {
        for (Throwable t = excepcion; t != null; t = t.getCause()) {
            for (Class<? extends Exception> tipo : IRRECUPERABLES) {
                if (tipo.isInstance(t)) {
                    return true;
                }
            }
        }
        return false;
    }
}
