package cl.andesstay.report.service;

import cl.andesstay.report.domain.ReservationStat;
import cl.andesstay.report.messaging.events.ReservationEvent;
import cl.andesstay.report.repository.ReservationStatRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Guarda cada evento en la tabla de agregaciones que alimenta los KPIs historicos.
 *
 * Es la mitad "hacia atras" de la reporteria: lo que permite preguntar por los ultimos treinta
 * dias. La mitad "ahora" la lleva Kafka Streams en KpiEnVivoService. Las dos leen el mismo tema
 * con grupos distintos y ninguna le quita nada a la otra.
 *
 * El consumidor no sabe nada de esto y este servicio no sabe nada de Kafka: recibe un evento ya
 * deserializado y decide que hacer con el.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IngestaDeEventosService {

    private final ReservationStatRepository repository;

    /**
     * Registra un evento una sola vez.
     *
     * Kafka entrega al menos una vez: un reequilibrio o un reinicio antes de confirmar el offset
     * hacen que el mismo mensaje vuelva. Descartarlo por eventId es lo que impide que una reserva
     * se cuente dos veces en los KPIs.
     *
     * @return true si el evento se registro, false si ya estaba
     */
    @Transactional
    public boolean registrar(ReservationEvent evento) {
        validar(evento);

        if (repository.existsByEventId(evento.eventId())) {
            log.warn("[Report] Evento duplicado ignorado eventId={} tipo={}",
                    evento.eventId(), evento.type());
            return false;
        }

        ReservationStat stat = ReservationStat.builder()
                .eventId(evento.eventId())
                .eventType(evento.type())
                .reservationId(evento.payload().reservationId())
                .unitId(evento.payload().unitId())
                .occurredAt(momentoDelHecho(evento.timestamp()))
                .build();

        repository.save(stat);

        log.debug("[Report] Registrado {} de la reserva {}",
                stat.getEventType(), stat.getReservationId());
        return true;
    }

    /**
     * Lo que no se puede agregar.
     *
     * IllegalArgumentException a proposito: es un fallo del mensaje y no del sistema, asi que el
     * manejador de errores lo manda al tema de descarte en el primer intento en vez de gastar
     * reintentos en algo que nunca va a mejorar.
     */
    private void validar(ReservationEvent evento) {
        if (evento == null) {
            throw new IllegalArgumentException("El evento llego vacio.");
        }
        if (evento.eventId() == null || evento.eventId().isBlank()) {
            throw new IllegalArgumentException("El evento no trae eventId: sin el no hay idempotencia.");
        }
        if (evento.type() == null || evento.type().isBlank()) {
            throw new IllegalArgumentException("El evento no trae tipo.");
        }
        if (evento.payload() == null || evento.payload().reservationId() == null) {
            throw new IllegalArgumentException("El evento no identifica ninguna reserva.");
        }
    }

    /**
     * Cuando ocurrio, no cuando se consumio.
     *
     * Antes se guardaba LocalDateTime.now() del consumidor. Con el consumo al dia la diferencia es
     * de milisegundos, pero basta un rato de retraso o una relectura del tema desde el principio
     * para que toda la serie de "reservas por hora" se amontone en la hora de la relectura.
     */
    private LocalDateTime momentoDelHecho(Instant instante) {
        Instant efectivo = instante == null ? Instant.now() : instante;
        return LocalDateTime.ofInstant(efectivo, ZoneId.systemDefault());
    }
}
