package cl.andesstay.report.consumer;

import cl.andesstay.report.config.KafkaConsumerConfig;
import cl.andesstay.report.config.KafkaErrorConfig;
import cl.andesstay.report.domain.ReservationStat;
import cl.andesstay.report.repository.ReservationStatRepository;
import cl.andesstay.report.service.IngestaDeEventosService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Lo que pasa con un evento que report no puede procesar, contra un broker de verdad.
 *
 * POR QUE EXISTE. El productor del tema de descarte serializaba solo bytes. Eso funciona con un JSON
 * mal formado, porque lo que se reenvia son los bytes originales, pero no cuando el JSON es valido y
 * lo que falla es el negocio o la base: en ese punto el valor del registro ya es un ReservationEvent
 * y ByteArraySerializer no sabe escribirlo. La publicacion en la DLT fallaba, el registro volvia a
 * entregarse sin fin y la particion quedaba bloqueada: los KPI historicos dejaban de avanzar.
 *
 * POR QUE UN BROKER EMBEBIDO Y NO MOCKS. El fallo esta en la serializacion del productor de la DLT y
 * en como el contenedor reacciona cuando esa publicacion falla. Con mocks no se ve ninguna de las dos
 * cosas. IngestaDeEventosService es el real, porque es quien lanza la IllegalArgumentException;
 * solo el repositorio es un doble, para no necesitar MySQL.
 *
 * POR QUE UNA PARTICION Y ORDEN FIJO. Con una sola particion, los mensajes se consumen uno detras de
 * otro: si el evento sin eventId la bloqueara, el valido del tercer caso no llegaria nunca. Es la
 * forma directa de probar que el consumo sigue.
 */
@SpringBootTest(classes = ReservationEventConsumerDescarteTest.Contexto.class)
@EmbeddedKafka(partitions = 1,
        topics = {ReservationEventConsumerDescarteTest.TEMA, ReservationEventConsumerDescarteTest.TEMA_DLT},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@ExtendWith(OutputCaptureExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext
class ReservationEventConsumerDescarteTest {

    static final String TEMA = "reservations.events";
    static final String TEMA_DLT = "reservations.events.DLT";

    private static final Duration ESPERA = Duration.ofSeconds(30);
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Solo la mitad Kafka del servicio: configuracion de errores, del consumidor, el listener y el
     * servicio de ingesta. Sin Streams, sin JPA y sin la API web, que no tienen nada que ver aqui.
     */
    @Configuration
    @EnableKafka
    @Import({KafkaErrorConfig.class, KafkaConsumerConfig.class,
            ReservationEventConsumer.class, IngestaDeEventosService.class})
    static class Contexto {
    }

    @Autowired
    private EmbeddedKafkaBroker broker;

    @MockBean
    private ReservationStatRepository repository;

    @SpyBean
    private IngestaDeEventosService ingesta;

    @Test
    @Order(1)
    @DisplayName("JSON mal formado: va a la DLT con los bytes originales, sin reintentar")
    void jsonMalFormado(CapturedOutput salida) throws Exception {
        byte[] original = "{ esto no es json".getBytes(StandardCharsets.UTF_8);
        publicar("malformado", original);

        ConsumerRecord<String, byte[]> descartado = esperarEnDescarte("malformado");

        assertThat(descartado.value()).isEqualTo(original);
        assertThat(cabecera(descartado, KafkaHeaders.DLT_EXCEPTION_FQCN)
                + cabecera(descartado, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .contains("DeserializationException");
        assertThat(lineaDeDescarte(salida, descartado))
                .contains("sin reintentar", "topic=" + TEMA, "particion=0")
                .doesNotContain("tras");
    }

    @Test
    @Order(2)
    @DisplayName("JSON valido sin eventId: va a la DLT como JSON legible tras un solo intento")
    void sinEventId(CapturedOutput salida) throws Exception {
        publicar("sin-event-id", evento(null, 42));

        ConsumerRecord<String, byte[]> descartado = esperarEnDescarte("sin-event-id");

        // Legible: se puede parsear y conserva el contenido que llego.
        JsonNode cuerpo = JSON.readTree(descartado.value());
        assertThat(cuerpo.path("type").asText()).isEqualTo("ReservationCreated");
        assertThat(cuerpo.path("payload").path("reservationId").asLong()).isEqualTo(42L);
        assertThat(cuerpo.path("eventId").isNull() || cuerpo.path("eventId").isMissingNode()).isTrue();
        // ISO 8601, como fija el contrato del envelope, y no milisegundos desde 1970.
        assertThat(cuerpo.path("timestamp").asText()).isEqualTo("2026-11-21T10:00:00Z");
        assertThat(cabecera(descartado, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .contains("IllegalArgumentException");

        // IllegalArgumentException esta entre las que no se reintentan: una sola entrega.
        verify(ingesta, times(1)).registrar(argThat(e -> e != null && e.eventId() == null));

        assertThat(lineaDeDescarte(salida, descartado))
                .contains("sin reintentar", "topic=" + TEMA, "particion=0",
                        "IllegalArgumentException", "eventId")
                .doesNotContain("tras");
    }

    @Test
    @Order(3)
    @DisplayName("Un evento valido enviado despues se procesa: la particion no quedo bloqueada")
    void particionSigueAvanzando() throws Exception {
        publicar("valido", evento("e-valido", 43));

        verify(repository, timeout(ESPERA.toMillis()))
                .save(argThat((ReservationStat s) -> s != null && "e-valido".equals(s.getEventId())));
    }

    @Test
    @Order(4)
    @DisplayName("Fallo de la base: se reintenta 3 veces y va a la DLT como JSON")
    void falloDeLaBaseSeReintenta(CapturedOutput salida) throws Exception {
        doThrow(new DataAccessResourceFailureException("La base no responde"))
                .when(repository).save(argThat((ReservationStat s) -> s != null && "e-bd-caida".equals(s.getEventId())));

        publicar("bd-caida", evento("e-bd-caida", 44));

        ConsumerRecord<String, byte[]> descartado = esperarEnDescarte("bd-caida");

        assertThat(JSON.readTree(descartado.value()).path("eventId").asText()).isEqualTo("e-bd-caida");
        verify(ingesta, times(3)).registrar(argThat(e -> e != null && "e-bd-caida".equals(e.eventId())));
        assertThat(lineaDeDescarte(salida, descartado))
                .contains("tras 3 intentos", "topic=" + TEMA, "particion=0", "La base no responde");
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────────────────────────

    private static String evento(String eventId, long reservationId) {
        String id = eventId == null ? "" : "\"eventId\":\"" + eventId + "\",";
        return "{\"type\":\"ReservationCreated\"," + id
                + "\"timestamp\":\"2026-11-21T10:00:00Z\",\"source\":\"ms-andesstay-reservations\","
                + "\"actor\":{\"userId\":\"u-1\",\"role\":\"Guest\",\"tenant\":\"andesstay\"},"
                + "\"payload\":{\"reservationId\":" + reservationId + ",\"unitId\":7,"
                + "\"guestId\":\"g-1\",\"estado\":\"PENDING\"}}";
    }

    private void publicar(String clave, String valor) throws Exception {
        publicar(clave, valor.getBytes(StandardCharsets.UTF_8));
    }

    private void publicar(String clave, byte[] valor) throws Exception {
        Map<String, Object> props = KafkaTestUtils.producerProps(broker);
        try (Producer<String, byte[]> productor = new DefaultKafkaProducerFactory<>(props,
                new StringSerializer(), new ByteArraySerializer()).createProducer()) {
            productor.send(new ProducerRecord<>(TEMA, clave, valor)).get();
        }
    }

    /** Lee la DLT desde el principio con un grupo propio hasta encontrar el mensaje de esa clave. */
    private ConsumerRecord<String, byte[]> esperarEnDescarte(String clave) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("lector-dlt-" + UUID.randomUUID(), "false", broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, byte[]> lector = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new ByteArrayDeserializer()).createConsumer()) {
            lector.subscribe(List.of(TEMA_DLT));
            long limite = System.currentTimeMillis() + ESPERA.toMillis();
            while (System.currentTimeMillis() < limite) {
                for (ConsumerRecord<String, byte[]> r : lector.poll(Duration.ofMillis(500))) {
                    if (clave.equals(r.key())) {
                        return r;
                    }
                }
            }
        }
        throw new AssertionError("No llego a " + TEMA_DLT + " ningun mensaje con clave " + clave);
    }

    private static String cabecera(ConsumerRecord<?, ?> registro, String nombre) {
        Header h = registro.headers().lastHeader(nombre);
        return h == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** La linea de log del descarte de ese offset concreto, para no confundirla con la de otro caso. */
    private static String lineaDeDescarte(CapturedOutput salida, ConsumerRecord<?, ?> descartado) {
        long offsetOriginal = ByteBuffer.wrap(
                descartado.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong();
        return Arrays.stream(salida.getOut().split("\\R"))
                .filter(l -> l.contains("Mensaje descartado") && l.contains("offset=" + offsetOriginal + " "))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No hay linea de descarte para offset=" + offsetOriginal));
    }
}
