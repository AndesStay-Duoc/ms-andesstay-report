package cl.andesstay.report.repository;

import cl.andesstay.report.domain.ReservationStat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface ReservationStatRepository extends JpaRepository<ReservationStat, Long> {

    boolean existsByEventId(String eventId);

    /** Reservas creadas por hora (últimas N horas) */
    @Query("""
        SELECT FUNCTION('TO_CHAR', s.occurredAt, 'YYYY-MM-DD HH24') as hour,
               COUNT(s) as total
        FROM ReservationStat s
        WHERE s.eventType = 'CREATED'
          AND s.occurredAt >= :from
        GROUP BY FUNCTION('TO_CHAR', s.occurredAt, 'YYYY-MM-DD HH24')
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

    /** Reservas activas en este momento */
    @Query("SELECT COUNT(s) FROM ReservationStat s WHERE s.eventType = 'EN_ESTADÍA'")
    long activeReservations();
}
