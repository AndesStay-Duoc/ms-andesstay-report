package cl.andesstay.report.streams;

import cl.andesstay.report.messaging.events.Actor;
import cl.andesstay.report.messaging.events.ReservationEvent;
import cl.andesstay.report.messaging.events.ReservationPayload;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Los tres KPIs del caso, calculados por la topologia real y sin broker.
 *
 * TopologyTestDriver ejecuta la misma topologia que corre en produccion —los mismos Serde, el
 * mismo extractor de tiempo, las mismas ventanas— contra temas y almacenes en memoria. Es lo que
 * permite comprobar en milisegundos que el tiempo de ciclo se calcula bien o que una reserva
 * cancelada deja de contar como ocupacion, sin levantar nada.
 */
class KpiTopologyTest {

    private static final String TEMA = "reservations.events";
    private static final Instant BASE = Instant.parse("2026-11-17T12:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, ReservationEvent> entrada;

    @BeforeEach
    void montarLaTopologia() {
        SerdesDeReporteria serdes = new SerdesDeReporteria();
        KpiTopology topologia = new KpiTopology();
        ReflectionTestUtils.setField(topologia, "temaDeEventos", TEMA);

        StreamsBuilder builder = new StreamsBuilder();
        topologia.kpiStream(builder, serdes);

        Properties propiedades = new Properties();
        propiedades.put(StreamsConfig.APPLICATION_ID_CONFIG, "prueba-kpis");
        propiedades.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "no-se-usa:9092");

        driver = new TopologyTestDriver(builder.build(), propiedades);
        entrada = driver.createInputTopic(TEMA,
                Serdes.String().serializer(), serdes.reservationEvent().serializer());
    }

    @AfterEach
    void cerrar() {
        driver.close();
    }

    @Nested
    @DisplayName("Ocupación activa")
    class Ocupacion {

        @Test
        @DisplayName("Una reserva en estadía cuenta; una que ya hizo checkout, no")
        void soloLasQueSiguenDentro() {
            recorrido("1", BASE, "EN_ESTADIA");
            recorrido("2", BASE, "CHECKOUT");

            assertThat(enEstadia()).isEqualTo(1);
        }

        @Test
        @DisplayName("Una reserva cancelada deja de contar")
        void laCanceladaNoCuenta() {
            publicar("3", BASE, null, "CREADA");
            publicar("3", BASE.plusSeconds(60), "CREADA", "CANCELADA");

            assertThat(enEstadia()).isZero();
        }

        @Test
        @DisplayName("Varios eventos de la misma reserva dejan un solo acumulado")
        void unAcumuladoPorReserva() {
            recorrido("1", BASE, "EN_ESTADIA");
            recorrido("1", BASE.plusSeconds(600), "EN_ESTADIA");

            assertThat(ciclos()).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Tiempo de ciclo")
    class TiempoDeCiclo {

        @Test
        @DisplayName("Se mide de la creación al checkout, con los instantes reales")
        void deLaCreacionAlCierre() {
            publicar("1", BASE, null, "CREADA");
            publicar("1", BASE.plusSeconds(90 * 60), "EN_ESTADIA", "CHECKOUT");

            assertThat(ciclo("1").minutosDeCiclo()).isEqualTo(90L);
        }

        @Test
        @DisplayName("Una reserva todavía abierta no tiene tiempo de ciclo")
        void laAbiertaNoTieneCiclo() {
            publicar("1", BASE, null, "CREADA");
            publicar("1", BASE.plusSeconds(60), "CREADA", "CONFIRMADA");

            assertThat(ciclo("1").minutosDeCiclo()).isNull();
        }

        @Test
        @DisplayName("Una cancelación también cierra el ciclo")
        void laCancelacionCierra() {
            publicar("1", BASE, null, "CREADA");
            publicar("1", BASE.plusSeconds(30 * 60), "CREADA", "CANCELADA");

            assertThat(ciclo("1").minutosDeCiclo()).isEqualTo(30L);
        }

        @Test
        @DisplayName("El instante de creación no se pisa si el evento se repite")
        void laCreacionNoSePisa() {
            publicar("1", BASE, null, "CREADA");
            publicar("1", BASE.plusSeconds(600), null, "CREADA");
            publicar("1", BASE.plusSeconds(60 * 60), "EN_ESTADIA", "CHECKOUT");

            assertThat(ciclo("1").minutosDeCiclo()).isEqualTo(60L);
        }
    }

    @Nested
    @DisplayName("Reservas por hora")
    class PorHora {

        @Test
        @DisplayName("Solo cuentan las creaciones, no cada transición")
        void soloLasCreaciones() {
            publicar("1", BASE, null, "CREADA");
            publicar("2", BASE.plusSeconds(300), null, "CREADA");
            publicar("1", BASE.plusSeconds(600), "CREADA", "CONFIRMADA");

            assertThat(cuentasPorVentana()).containsExactly(2L);
        }

        @Test
        @DisplayName("Dos horas distintas caen en ventanas distintas")
        void unaVentanaPorHora() {
            publicar("1", BASE, null, "CREADA");
            publicar("2", BASE.plusSeconds(3600), null, "CREADA");
            publicar("3", BASE.plusSeconds(3900), null, "CREADA");

            assertThat(cuentasPorVentana()).containsExactly(1L, 2L);
        }

        @Test
        @DisplayName("La ventana la decide el instante del evento, no el del consumo")
        void agrupaPorElInstanteDelEvento() {
            // Los dos se publican ahora, pero dicen haber ocurrido con dos horas de diferencia.
            publicar("1", BASE, null, "CREADA");
            publicar("2", BASE.plusSeconds(7200), null, "CREADA");

            assertThat(cuentasPorVentana()).hasSize(2);
        }
    }

    @Test
    @DisplayName("Un evento sin reserva no entra en ningún KPI")
    void descartaLoQueNoIdentificaReserva() {
        ReservationEvent sinReserva = new ReservationEvent(
                "reservation.created", "evt-x", BASE, "t", "c", "origen",
                new Actor("sub", "Operador", "staff"),
                new ReservationPayload(null, null, null, null, null, null, null,
                        "CREADA", null, null, null, null, null));

        entrada.pipeInput("sin-id", sinReserva);

        assertThat(ciclos()).isEmpty();
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    private void recorrido(String id, Instant desde, String hasta) {
        publicar(id, desde, null, "CREADA");
        if (!"CREADA".equals(hasta)) {
            publicar(id, desde.plusSeconds(3600), "CREADA", hasta);
        }
    }

    private void publicar(String id, Instant cuando, String desde, String hacia) {
        ReservationEvent evento = new ReservationEvent(
                "reservation." + hacia.toLowerCase(),
                "evt-" + id + "-" + cuando.toEpochMilli(),
                cuando, "traza", "corr-" + id, "ms-andesstay-reservations",
                new Actor("sub-operador", "Operador", "staff"),
                new ReservationPayload(Long.valueOf(id), 7L, "sub-huesped", "h@andesstay.cl",
                        LocalDate.of(2026, 3, 10), LocalDate.of(2026, 3, 13), 2,
                        hacia, desde, null, null, null, null));

        entrada.pipeInput(id, evento, cuando);
    }

    private KeyValueStore<String, CicloDeReserva> almacenDeCiclos() {
        return driver.getKeyValueStore(KpiTopology.ALMACEN_DE_CICLOS);
    }

    private List<CicloDeReserva> ciclos() {
        List<CicloDeReserva> todos = new ArrayList<>();
        try (KeyValueIterator<String, CicloDeReserva> it = almacenDeCiclos().all()) {
            while (it.hasNext()) {
                todos.add(it.next().value);
            }
        }
        return todos;
    }

    private CicloDeReserva ciclo(String id) {
        return almacenDeCiclos().get(id);
    }

    private long enEstadia() {
        return ciclos().stream().filter(CicloDeReserva::estaEnEstadia).count();
    }

    /** Las cuentas de cada ventana, en orden cronológico. */
    private List<Long> cuentasPorVentana() {
        WindowStore<String, Long> almacen = driver.getWindowStore(KpiTopology.ALMACEN_POR_HORA);
        List<Long> cuentas = new ArrayList<>();
        try (KeyValueIterator<Windowed<String>, Long> it =
                     almacen.fetchAll(Instant.EPOCH, BASE.plusSeconds(86_400))) {
            while (it.hasNext()) {
                cuentas.add(it.next().value);
            }
        }
        return cuentas;
    }
}
