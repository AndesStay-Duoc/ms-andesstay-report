package cl.andesstay.report.consumer;

import cl.andesstay.report.messaging.events.ReservationEvent;
import cl.andesstay.report.service.IngestaDeEventosService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consume reservations.events y guarda cada evento para el historico.
 *
 * ESTA CLASE NO TIENE LOGICA. Recibe, delega y confirma. La idempotencia, la validacion y el mapeo
 * viven en IngestaDeEventosService, que se puede probar sin broker.
 *
 * LOS ERRORES YA NO SE TRAGAN. Antes el metodo envolvia todo en un try y, al fallar, registraba el
 * error y no confirmaba el offset: eso no reintenta, bloquea. La particion volvia a entregar el
 * mismo mensaje una y otra vez y los siguientes no llegaban nunca. Ahora la excepcion sale del
 * metodo y la recoge DefaultErrorHandler, que reintenta con espera creciente y, agotados los
 * intentos, manda el mensaje a reservations.events.DLT y deja avanzar la particion.
 *
 * EL GRUPO ES PROPIO. Kafka Streams lee este mismo tema con su application-id para mantener los
 * KPIs en vivo. Dos grupos distintos sobre el mismo tema reciben cada uno todos los mensajes: es
 * lo que una cola tradicional no permite y la razon de que el historico y el tiempo real puedan
 * convivir sin estorbarse.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationEventConsumer {

    private final IngestaDeEventosService ingesta;

    @KafkaListener(
            topics = "${andesstay.kafka.topics.reservations-events}",
            groupId = "${andesstay.kafka.groups.report}",
            containerFactory = "eventosKafkaListenerContainerFactory",
            // Misma bandera que apaga la topologia de Streams: o el servicio habla con Kafka o no
            // lo hace, y no tiene sentido que una mitad arranque sin la otra.
            autoStartup = "${andesstay.kafka.enabled:true}")
    public void consumir(ConsumerRecord<String, ReservationEvent> registro, Acknowledgment acuse) {
        ReservationEvent evento = registro.value();

        log.debug("[Report] Recibido eventId={} particion={} offset={}",
                evento == null ? "null" : evento.eventId(), registro.partition(), registro.offset());

        ingesta.registrar(evento);
        acuse.acknowledge();
    }
}
