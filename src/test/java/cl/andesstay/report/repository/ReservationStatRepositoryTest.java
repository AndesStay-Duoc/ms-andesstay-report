package cl.andesstay.report.repository;

import cl.andesstay.report.domain.ReservationStat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Las consultas de KPIs contra un MySQL 8 real, el mismo motor que el despliegue.
 *
 * Tiene que ser MySQL y no una base en memoria: el error que corrige esta prueba era que
 * reservationsPerHour usaba TO_CHAR, que Hibernate aceptaba al arrancar y MySQL rechazaba al
 * ejecutar. Solo se ve ejecutando el SQL en MySQL.
 *
 * Necesita Docker. Sin Docker la clase se omite en lugar de fallar, para que mvn test siga
 * funcionando en un equipo que no lo tenga.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers(disabledWithoutDocker = true)
class ReservationStatRepositoryTest {

    @Container
    @ServiceConnection
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    private static final LocalDateTime AHORA = LocalDateTime.of(2026, 10, 15, 14, 30);

    @Autowired
    private ReservationStatRepository repositorio;

    @BeforeEach
    void limpiar() {
        repositorio.deleteAll();
    }

    private void evento(String tipo, long reserva, long unidad, LocalDateTime cuando) {
        repositorio.save(ReservationStat.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(tipo)
                .reservationId(reserva)
                .unitId(unidad)
                .occurredAt(cuando)
                .build());
    }

    @Test
    @DisplayName("reservas por hora se ejecuta en MySQL y agrupa por \"yyyy-MM-dd HH\"")
    void reservasPorHora() {
        evento("reservation.created", 1, 10, AHORA.withMinute(5));
        evento("reservation.created", 2, 10, AHORA.withMinute(55));
        evento("reservation.created", 3, 11, AHORA.plusHours(1));
        evento("reservation.confirmed", 1, 10, AHORA.withMinute(20));   // no es una creación: no cuenta
        evento("reservation.created", 4, 11, AHORA.minusDays(2));        // fuera del rango pedido

        List<Object[]> filas = repositorio.reservationsPerHour(AHORA.minusHours(1));

        assertEquals(2, filas.size(), "dos horas con reservas creadas en el rango");
        assertArrayEquals(new Object[]{"2026-10-15 14", 2L}, filas.get(0));
        assertArrayEquals(new Object[]{"2026-10-15 15", 1L}, filas.get(1));
    }

    @Test
    @DisplayName("en estadía cuenta reservas distintas que todavía no han hecho checkout")
    void reservasEnEstadia() {
        evento("reservation.in_stay", 1, 10, AHORA);
        evento("reservation.in_stay", 1, 10, AHORA.plusMinutes(1));  // evento repetido: sigue siendo 1 reserva
        evento("reservation.in_stay", 2, 10, AHORA);
        evento("reservation.in_stay", 3, 11, AHORA);
        evento("reservation.checked_out", 3, 11, AHORA.plusDays(2));       // ya salió: no cuenta
        evento("reservation.confirmed", 4, 11, AHORA);                 // nunca entró: no cuenta

        assertEquals(2, repositorio.activeReservations());
    }

    @Test
    @DisplayName("las unidades más reservadas salen ordenadas de mayor a menor")
    void unidadesMasReservadas() {
        evento("reservation.created", 1, 10, AHORA);
        evento("reservation.created", 2, 11, AHORA);
        evento("reservation.created", 3, 11, AHORA);

        List<Object[]> filas = repositorio.topUnits(AHORA.minusHours(1));

        assertArrayEquals(new Object[]{11L, 2L}, filas.get(0));
        assertArrayEquals(new Object[]{10L, 1L}, filas.get(1));
    }
}
