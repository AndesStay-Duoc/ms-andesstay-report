package cl.andesstay.report.domain;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * Tabla de agregaciones para KPIs.
 * Se actualiza en tiempo real al consumir eventos de Kafka.
 */
@Entity
@Table(name = "REPORT_STATS")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class ReservationStat {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "stat_seq")
    @SequenceGenerator(name = "stat_seq", sequenceName = "SEQ_REPORT_STATS", allocationSize = 1)
    private Long id;

    @Column(name = "EVENT_ID", nullable = false, unique = true, length = 36)
    private String eventId;

    @Column(name = "EVENT_TYPE", nullable = false, length = 50)
    private String eventType;

    @Column(name = "RESERVATION_ID", nullable = false)
    private Long reservationId;

    @Column(name = "UNIT_ID")
    private Long unitId;

    @Column(name = "OCCURRED_AT")
    private LocalDateTime occurredAt;

    @Column(name = "PERSISTED_AT")
    private LocalDateTime persistedAt;

    @PrePersist
    protected void onCreate() {
        persistedAt = LocalDateTime.now();
    }
}
