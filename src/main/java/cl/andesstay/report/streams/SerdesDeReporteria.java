package cl.andesstay.report.streams;

import cl.andesstay.report.messaging.events.ReservationEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Los Serde de JSON de la topologia, en un solo sitio.
 *
 * ignoreTypeHeaders() porque el productor no manda la cabecera con el nombre de la clase: cada
 * repositorio tiene su copia del record y los paquetes no tienen que coincidir. Los paquetes de
 * confianza se limitan a cl.andesstay en vez del "*" que habia antes, que desactiva la proteccion
 * de JsonDeserializer y permite instanciar cualquier clase del classpath desde un mensaje.
 *
 * El Serde de CicloDeReserva es para el almacen de estado, no para ningun tema de negocio: Kafka
 * Streams lo usa al materializar el acumulado y al respaldarlo en su changelog.
 */
@Configuration
public class SerdesDeReporteria {

    private static final String PAQUETES_DE_CONFIANZA = "cl.andesstay.*";

    @Bean
    public JsonSerde<ReservationEvent> reservationEvent() {
        return serdeDe(ReservationEvent.class);
    }

    @Bean
    public JsonSerde<CicloDeReserva> cicloDeReserva() {
        return serdeDe(CicloDeReserva.class);
    }

    private static <T> JsonSerde<T> serdeDe(Class<T> tipo) {
        JsonSerde<T> serde = new JsonSerde<>(tipo);
        serde.deserializer().addTrustedPackages(PAQUETES_DE_CONFIANZA);
        return serde.ignoreTypeHeaders();
    }
}
