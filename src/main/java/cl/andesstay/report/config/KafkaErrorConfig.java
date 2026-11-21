package cl.andesstay.report.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
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

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * El productor del tema de descarte serializa bytes crudos.
     *
     * Tiene que ser asi: lo que se descarta puede ser precisamente un mensaje que no se pudo
     * deserializar, y volver a serializarlo con JSON fallaria otra vez. Reenviar los bytes tal
     * como llegaron conserva la evidencia intacta.
     */
    @Bean
    public KafkaTemplate<Object, Object> descarteKafkaTemplate() {
        Map<String, Object> propiedades = new HashMap<>();
        propiedades.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propiedades.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propiedades.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        propiedades.put(ProducerConfig.ACKS_CONFIG, "all");
        propiedades.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(propiedades));
    }

    /**
     * El nombre lleva el sufijo Kafka a proposito. Spring nombra los beans por el metodo, y
     * "manejadorDeErrores" ya es el nombre del @RestControllerAdvice web.ManejadorDeErrores: con
     * los dos iguales y la sobrescritura prohibida en Spring Boot, el servicio no arrancaba.
     * KafkaErrorConfigContextoTest carga las dos clases juntas para que no vuelva a pasar.
     */
    @Bean
    public CommonErrorHandler manejadorDeErroresKafka(KafkaTemplate<Object, Object> descarteKafkaTemplate) {

        DeadLetterPublishingRecoverer descarte = new DeadLetterPublishingRecoverer(
                descarteKafkaTemplate,
                (registro, excepcion) -> {
                    log.error("[Report] Mensaje descartado tras {} intentos: topic={} particion={} offset={} causa={}",
                            INTENTOS, registro.topic(), registro.partition(), registro.offset(),
                            excepcion.getMessage());
                    return new TopicPartition(registro.topic() + SUFIJO_DE_DESCARTE, registro.partition());
                });

        ExponentialBackOff escalera = new ExponentialBackOff(ESPERA_INICIAL_MS, MULTIPLICADOR);
        escalera.setMaxInterval(ESPERA_MAXIMA_MS);
        escalera.setMaxAttempts(INTENTOS - 1);

        DefaultErrorHandler manejador = new DefaultErrorHandler(descarte, escalera);

        // Sin vuelta de hoja: un mensaje con formato invalido o sin los campos obligatorios no
        // mejora por esperar. Va al tema de descarte en el primer intento.
        manejador.addNotRetryableExceptions(
                DeserializationException.class,
                IllegalArgumentException.class,
                org.springframework.kafka.support.converter.ConversionException.class);

        manejador.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);
        return manejador;
    }
}
