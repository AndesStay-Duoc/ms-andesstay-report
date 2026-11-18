package cl.andesstay.report.repository;

import cl.andesstay.report.domain.ReservationStat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface ReservationStatRepository extends JpaRepository<ReservationStat, Long> {

    boolean existsByEventId(String eventId);

    /**
     * Reservas creadas por hora (últimas N horas).
     *
     * DATE_FORMAT y no TO_CHAR: la base es MySQL, que no tiene TO_CHAR (es de Oracle). El HQL se
     * validaba al arrancar, pero MySQL rechazaba el SQL al ejecutarlo y GET /kpis respondía 500
     * en cuanto había un evento CREATED. La salida conserva la forma "yyyy-MM-dd HH".
     */
    @Query("""
        SELECT FUNCTION('DATE_FORMAT', s.occurredAt, '%Y-%m-%d %H') as hour,
               COUNT(s) as total
        FROM ReservationStat s
        WHERE s.eventType = 'reservation.created'
          AND s.occurredAt >= :from
        GROUP BY FUNCTION('DATE_FORMAT', s.occurredAt, '%Y-%m-%d %H')
        ORDER BY 1 ASC
    """)
    List<Object[]> reservationsPerHour(@Param("from") LocalDateTime from);

    /** Unidades con más reservas (top N) */
    @Query("""
        SELECT s.unitId, COUNT(s) as total
        FROM ReservationStat s
        WHERE s.eventType = 'reservation.created'
          AND s.occurredAt >= :from
        GROUP BY s.unitId
        ORDER BY 2 DESC
    """)
    List<Object[]> topUnits(@Param("from") LocalDateTime from);

    /**
     * Reservas en estadía en este momento: entraron en estadía y todavía no han hecho checkout.
     *
     * EL VOCABULARIO. Esta consulta llegó a aceptar 'EN_ESTADIA' y 'EN_ESTADÍA' a la vez porque el
     * productor mandaba el nombre del enum y ese nombre cambió: el KPI quedó fijo en 0 hasta que
     * alguien lo notó. El parche sobra desde que el tipo del evento lo fija TipoDeEvento en
     * ms-andesstay-reservations y vale lo que dice el contrato, que no depende de cómo se llame un
     * enum en el dominio. La tabla está vacía —Kafka nunca se encendió— así que no hay nada que
     * migrar.
     *
     * Desde la estadía la única transición es el checkout (no se puede cancelar), así que basta
     * con descartar las reservas que ya lo tienen.
     */
    @Query("""
        SELECT COUNT(DISTINCT s.reservationId)
        FROM ReservationStat s
        WHERE s.eventType = 'reservation.in_stay'
          AND NOT EXISTS (
              SELECT c.id FROM ReservationStat c
              WHERE c.reservationId = s.reservationId
                AND c.eventType = 'reservation.checked_out')
    """)
    long activeReservations();
}
