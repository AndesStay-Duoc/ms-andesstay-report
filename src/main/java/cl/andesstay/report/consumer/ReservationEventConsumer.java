package cl.andesstay.report.consumer;

import cl.andesstay.report.domain.ReservationStat;
import cl.andesstay.report.repository.ReservationStatRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationEventConsumer {

    private final ReservationStatRepository repository;

    @KafkaListener(
        topics = "${andesstay.kafka.topics.reservations-events}",
        groupId = "report-consumer-group"
    )
    public void consume(ConsumerRecord<String, Map<String, Object>> record,
                        Acknowledgment acknowledgment) {
        Map<String, Object> event = record.value();
        String eventId = (String) event.get("eventId");

        try {
            if (repository.existsByEventId(eventId)) {
                log.warn("[Report] Evento duplicado ignorado eventId={}", eventId);
                acknowledgment.acknowledge();
                return;
            }

            ReservationStat stat = ReservationStat.builder()
                    .eventId(eventId)
                    .eventType((String) event.get("eventType"))
                    .reservationId(toLong(event.get("reservationId")))
                    .unitId(toLong(event.get("unitId")))
                    .occurredAt(LocalDateTime.now())
                    .build();

            repository.save(stat);
            acknowledgment.acknowledge();
            log.debug("[Report] Stat guardada → eventType={} reservationId={}",
                    stat.getEventType(), stat.getReservationId());

        } catch (Exception e) {
            log.error("[Report] Error procesando evento Kafka eventId={}: {}", eventId, e.getMessage());
        }
    }

    private Long toLong(Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.longValue();
        return Long.parseLong(value.toString());
    }
}
