package cl.andesstay.report.service;

import cl.andesstay.report.streams.CicloDeReserva;
import cl.andesstay.report.streams.KpiTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Los KPIs leidos directamente del estado de Kafka Streams.
 *
 * SON CONSULTAS INTERACTIVAS: el almacen que la topologia mantiene al dia se lee desde dentro del
 * proceso, sin ir al broker y sin tocar MySQL. Por eso responden en microsegundos y por eso el
 * panel puede refrescarse cada pocos segundos sin castigar a nadie.
 *
 * LO QUE ESTA CLASE NO PUEDE HACER TODAVIA. Con varias instancias del servicio, cada una guarda
 * las particiones que le tocaron, asi que responde con SU parte del estado. Para el tamaño de este
 * caso se despliega una sola instancia de reporteria y la respuesta es completa; con varias habria
 * que consultar a las demas por HTTP usando KafkaStreams.queryMetadataForKey. Queda dicho aqui
 * porque es la diferencia entre un numero correcto y uno que parece correcto.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KpiEnVivoService {

    private static final DateTimeFormatter FORMATO_HORA = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");

    private final StreamsBuilderFactoryBean streamsFactory;

    /** Si la topologia no esta corriendo, los KPIs en vivo no existen; mejor decirlo. */
    public boolean disponible() {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        return streams != null && streams.state().isRunningOrRebalancing();
    }

    /**
     * Cuantas reservas estan en estadia ahora mismo.
     *
     * Se cuenta sobre el ultimo estado conocido de cada reserva, que es lo que el almacen guarda.
     * La consulta equivalente en SQL tenia que descartar a mano las que ya habian hecho checkout
     * con un NOT EXISTS; aqui no hace falta, porque el acumulado ya refleja el estado actual.
     */
    public long ocupacionActiva() {
        return recorrerCiclos(iterador -> {
            long enEstadia = 0;
            while (iterador.hasNext()) {
                if (iterador.next().value.estaEnEstadia()) {
                    enEstadia++;
                }
            }
            return enEstadia;
        });
    }

    /**
     * El tiempo de ciclo medio, en minutos, de las reservas ya cerradas.
     *
     * Se promedian solo las cerradas: incluir las abiertas mediria cuanto llevan vivas, no cuanto
     * duraron. Devuelve null si todavia no hay ninguna cerrada, que es distinto de cero.
     */
    public Long tiempoDeCicloMedioEnMinutos() {
        return recorrerCiclos(iterador -> {
            long suma = 0;
            long cerradas = 0;
            while (iterador.hasNext()) {
                Long minutos = iterador.next().value.minutosDeCiclo();
                if (minutos != null) {
                    suma += minutos;
                    cerradas++;
                }
            }
            return cerradas == 0 ? null : suma / cerradas;
        });
    }

    /**
     * Reservas creadas por hora dentro del rango pedido.
     *
     * El almacen de ventanas guarda una cuenta por cada hora, ya calculada. Se devuelven en orden
     * cronologico y con la misma forma "yyyy-MM-dd HH" que usa el endpoint historico, para que el
     * panel pueda pintar las dos series sin traducir nada.
     */
    public List<Map<String, Object>> reservasPorHora(Duration rango) {
        ReadOnlyWindowStore<String, Long> almacen = almacenDeVentanas();
        if (almacen == null) {
            return List.of();
        }

        Instant hasta = Instant.now();
        Instant desde = hasta.minus(rango);

        List<Map<String, Object>> serie = new ArrayList<>();
        try (KeyValueIterator<Windowed<String>, Long> iterador = almacen.fetchAll(desde, hasta)) {
            while (iterador.hasNext()) {
                KeyValue<Windowed<String>, Long> ventana = iterador.next();
                LocalDateTime inicio = LocalDateTime.ofInstant(
                        ventana.key.window().startTime(), ZoneId.systemDefault());

                Map<String, Object> punto = new LinkedHashMap<>();
                punto.put("hour", inicio.format(FORMATO_HORA));
                punto.put("total", ventana.value);
                serie.add(punto);
            }
        }
        serie.sort((a, b) -> String.valueOf(a.get("hour")).compareTo(String.valueOf(b.get("hour"))));
        return serie;
    }

    /** Cuantas reservas conoce el almacen, util para entender de cuantos datos salen los KPIs. */
    public long reservasEnEstado() {
        return recorrerCiclos(iterador -> {
            long total = 0;
            while (iterador.hasNext()) {
                iterador.next();
                total++;
            }
            return total;
        });
    }

    // ── Interno ──────────────────────────────────────────────────────────────

    private interface LecturaDeCiclos<T> {
        T leer(KeyValueIterator<String, CicloDeReserva> iterador);
    }

    private <T> T recorrerCiclos(LecturaDeCiclos<T> lectura) {
        ReadOnlyKeyValueStore<String, CicloDeReserva> almacen = almacenDeCiclos();
        if (almacen == null) {
            return lectura.leer(VACIO);
        }
        try (KeyValueIterator<String, CicloDeReserva> iterador = almacen.all()) {
            return lectura.leer(iterador);
        }
    }

    private ReadOnlyKeyValueStore<String, CicloDeReserva> almacenDeCiclos() {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams == null || !streams.state().isRunningOrRebalancing()) {
            return null;
        }
        return streams.store(StoreQueryParameters.fromNameAndType(
                KpiTopology.ALMACEN_DE_CICLOS, QueryableStoreTypes.keyValueStore()));
    }

    private ReadOnlyWindowStore<String, Long> almacenDeVentanas() {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams == null || !streams.state().isRunningOrRebalancing()) {
            return null;
        }
        return streams.store(StoreQueryParameters.fromNameAndType(
                KpiTopology.ALMACEN_POR_HORA, QueryableStoreTypes.windowStore()));
    }

    /** Iterador vacio para cuando la topologia no esta corriendo: evita repartir nulos. */
    private static final KeyValueIterator<String, CicloDeReserva> VACIO =
            new KeyValueIterator<>() {
                @Override public void close() { }
                @Override public String peekNextKey() { throw new java.util.NoSuchElementException(); }
                @Override public boolean hasNext() { return false; }
                @Override public KeyValue<String, CicloDeReserva> next() {
                    throw new java.util.NoSuchElementException();
                }
            };
}
