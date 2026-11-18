package cl.andesstay.report.streams;

import cl.andesstay.report.messaging.events.ReservationEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Agrupa por el instante en que el hecho ocurrio, no por el que trae el registro de Kafka.
 *
 * POR QUE NO BASTA EL DEL REGISTRO. Kafka le pone al mensaje la hora del productor al enviarlo, y
 * normalmente coincide con la del hecho; pero coinciden por casualidad, no por contrato. En cuanto
 * algo reenvia un evento —una reposicion, una migracion entre clústeres, una reconstruccion desde
 * un respaldo— la hora del registro pasa a ser la del reenvio, y las ventanas de "reservas por
 * hora" se amontonarian todas en la hora de la operacion en vez de reconstruirse como fueron.
 *
 * El campo timestamp del sobre lo pone quien presencio el hecho y no cambia nunca. Es el unico que
 * permite afirmar que releer el tema da el mismo panel.
 *
 * Si un evento llegara sin instante, se usa el del registro como ultimo recurso: devolver un valor
 * negativo haria que Streams descartara el mensaje en silencio.
 */
public class InstanteDelEvento implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> registro, long anterior) {
        if (registro.value() instanceof ReservationEvent evento && evento.timestamp() != null) {
            return evento.timestamp().toEpochMilli();
        }
        return registro.timestamp();
    }
}
