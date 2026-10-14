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
        WHERE s.eventType = 'CREATED'
          AND s.occurredAt >= :from
        GROUP BY FUNCTION('DATE_FORMAT', s.occurredAt, '%Y-%m-%d %H')
        ORDER BY 1 ASC
    """)
    List<Object[]> reservationsPerHour(@Param("from") LocalDateTime from);

    /** Unidades con más reservas (top N) */
    @Query("""
        SELECT s.unitId, COUNT(s) as total
        FROM ReservationStat s
        WHERE s.eventType = 'CREATED'
          AND s.occurredAt >= :from
        GROUP BY s.unitId
        ORDER BY 2 DESC
    """)
    List<Object[]> topUnits(@Param("from") LocalDateTime from);

    /**
     * Reservas en estadía en este momento: entraron en EN_ESTADIA y todavía no tienen CHECKOUT.
     *
     * Antes contaba eventType = 'EN_ESTADÍA', con tilde. reservations publica el nombre del enum,
     * que desde que se quitó la tilde es EN_ESTADIA, así que el KPI quedó fijo en 0. Además
     * contaba eventos y no reservas, y nunca restaba las que ya habían salido.
     *
     * Se acepta también la forma con tilde por los eventos guardados antes de ese cambio.
     * Desde EN_ESTADIA la única transición es CHECKOUT (no se puede cancelar), así que basta
     * con descartar las que tienen ese evento.
     */
    @Query("""
        SELECT COUNT(DISTINCT s.reservationId)
        FROM ReservationStat s
        WHERE s.eventType IN ('EN_ESTADIA', 'EN_ESTADÍA')
          AND NOT EXISTS (
              SELECT c.id FROM ReservationStat c
              WHERE c.reservationId = s.reservationId
                AND c.eventType = 'CHECKOUT')
    """)
    long activeReservations();
}
