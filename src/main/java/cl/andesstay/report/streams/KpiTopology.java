package cl.andesstay.report.streams;

import cl.andesstay.report.messaging.events.ReservationEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * La topologia de Kafka Streams que mantiene los KPIs del panel al dia.
 *
 * LOS TRES KPIS QUE PIDE EL CASO salen de aqui, de dos almacenes de estado:
 *
 *   · Reservas por hora  → ALMACEN_POR_HORA, una cuenta por ventana de una hora.
 *   · Ocupacion activa   → ALMACEN_DE_CICLOS, contando las reservas en EN_ESTADIA.
 *   · Tiempo de ciclo    → ALMACEN_DE_CICLOS, promediando las ya cerradas.
 *
 * POR QUE STREAMS Y NO MAS SQL. Las consultas que ya existen recorren REPORT_STATS cada vez que
 * alguien abre el panel: el coste crece con el historico y el dato llega tan fresco como lo haya
 * dejado la ultima escritura. Con Streams el calculo ocurre UNA vez, cuando el evento llega, y la
 * consulta solo lee un valor ya calculado. Es la diferencia entre preguntar y estar enterado, y es
 * lo que el caso quiere decir con "datos por streaming sin bloquear el core".
 *
 * LAS DOS VIAS CONVIVEN. El @KafkaListener sigue guardando cada evento en MySQL y /kpis sigue
 * respondiendo el historico desde ahi. Streams alimenta /kpis/live. Son preguntas distintas:
 * una mira hacia atras sin limite, la otra mira el ahora sin esperar.
 *
 * EL TIEMPO ES EL DEL EVENTO, no el del consumo. La ventana de una hora agrupa por el instante en
 * que la reserva se creo, asi que releer el tema desde el principio reconstruye exactamente las
 * mismas ventanas. Con el tiempo de proceso, una relectura amontonaria todo el historico en la
 * hora actual.
 */
@Slf4j
@Configuration
public class KpiTopology {

    public static final String ALMACEN_DE_CICLOS = "kpi-ciclo-de-reserva";
    public static final String ALMACEN_POR_HORA = "kpi-reservas-por-hora";

    /** Ventana de agregacion de "reservas por hora". */
    public static final Duration VENTANA = Duration.ofHours(1);

    /**
     * Cuanto se conserva cada ventana cerrada antes de borrarse del almacen.
     *
     * Siete dias es lo que el panel ofrece como rango maximo, asi que es lo que hace falta
     * mantener. Guardar mas solo haria crecer el estado local sin que nadie lo consulte.
     */
    public static final Duration RETENCION_DE_VENTANAS = Duration.ofDays(7);

    /**
     * Margen para los eventos que llegan tarde.
     *
     * Un evento retrasado unos minutos todavia entra en la ventana a la que pertenece en vez de
     * perderse. Cinco minutos cubren un reinicio o un pico de lag sin retrasar el cierre de la
     * ventana mas de la cuenta.
     */
    public static final Duration TOLERANCIA = Duration.ofMinutes(5);

    /** Clave unica bajo la que se agrupan todas las reservas para contarlas por hora. */
    private static final String CLAVE_GLOBAL = "reservas";

    @Value("${andesstay.kafka.topics.reservations-events}")
    private String temaDeEventos;

    @Bean
    public KStream<String, ReservationEvent> kpiStream(StreamsBuilder builder,
                                                       SerdesDeReporteria serdes) {

        KStream<String, ReservationEvent> eventos = builder
                .stream(temaDeEventos, Consumed.with(Serdes.String(), serdes.reservationEvent())
                        .withTimestampExtractor(new InstanteDelEvento()))
                .filter((clave, evento) -> esUtilizable(clave, evento))
                .peek((clave, evento) -> log.debug(
                        "[Streams] {} de la reserva {}", evento.type(), clave));

        // ── Ocupacion activa y tiempo de ciclo ───────────────────────────────
        //
        // Los eventos ya vienen con el reservationId como clave, asi que groupByKey no necesita
        // reparticionar: el acumulado de cada reserva se construye en la particion donde ya estaba.
        eventos
                .groupByKey(Grouped.with(Serdes.String(), serdes.reservationEvent()))
                .aggregate(
                        CicloDeReserva::inicial,
                        (clave, evento, acumulado) -> acumulado.con(evento),
                        Materialized.<String, CicloDeReserva>as(
                                        Stores.persistentKeyValueStore(ALMACEN_DE_CICLOS))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.cicloDeReserva()));

        // ── Reservas por hora ────────────────────────────────────────────────
        //
        // Solo cuentan las creaciones: contar todas las transiciones diria "cuanta actividad hubo",
        // que es otra pregunta. Se reagrupan bajo una clave unica porque el KPI es global; eso las
        // lleva a una sola particion, que para una cuenta por hora no es un problema de escala y
        // es el precio de tener el total en un solo valor consultable.
        eventos
                .filter((clave, evento) -> evento.payload().estadoAnterior() == null)
                .groupBy((clave, evento) -> CLAVE_GLOBAL,
                        Grouped.with(Serdes.String(), serdes.reservationEvent()))
                .windowedBy(TimeWindows.ofSizeAndGrace(VENTANA, TOLERANCIA))
                .count(Materialized.<String, Long>as(
                                Stores.persistentWindowStore(
                                        ALMACEN_POR_HORA, RETENCION_DE_VENTANAS, VENTANA, false))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        return eventos;
    }

    /**
     * Lo que no sirve para ningun KPI.
     *
     * Los mismos cuatro campos que exige IngestaDeEventosService.validar, la otra mitad de la
     * reporteria. Antes aqui solo se pedian la reserva y el instante: un evento sin eventId o sin
     * tipo terminaba en la DLT por la via de MySQL pero contaba igual en /kpis/live, y las dos vias
     * daban cifras distintas. En la prueba de punta a punta, un mensaje sin eventId sumo una
     * "reserva 999" que no existia.
     *
     * Aqui no hay DLT: Streams no tiene un manejador de errores de negocio como el del listener, y
     * mandar el mismo mensaje a la DLT dos veces —una por cada via— duplicaria la evidencia. La DLT
     * ya la escribe el consumidor de MySQL; Streams solo lo descarta y lo deja en el log.
     */
    private static boolean esUtilizable(String clave, ReservationEvent evento) {
        if (evento == null || evento.payload() == null || evento.payload().reservationId() == null) {
            log.warn("[Streams] Evento descartado por no identificar ninguna reserva: clave={}", clave);
            return false;
        }
        if (evento.eventId() == null || evento.eventId().isBlank()) {
            log.warn("[Streams] Evento descartado por no traer eventId: clave={} tipo={}",
                    clave, evento.type());
            return false;
        }
        if (evento.type() == null || evento.type().isBlank()) {
            log.warn("[Streams] Evento descartado por no traer tipo: clave={}", clave);
            return false;
        }
        if (evento.timestamp() == null) {
            log.warn("[Streams] Evento descartado por no traer instante: clave={} tipo={}",
                    clave, evento.type());
            return false;
        }
        return true;
    }
}
