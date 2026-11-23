package cl.andesstay.report.service;

import cl.andesstay.report.domain.ReservationStat;
import cl.andesstay.report.messaging.events.Actor;
import cl.andesstay.report.messaging.events.ReservationEvent;
import cl.andesstay.report.messaging.events.ReservationPayload;
import cl.andesstay.report.repository.ReservationStatRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Las reglas de ingesta, sin broker y sin base de datos.
 *
 * Que se puedan probar asi es el motivo de haberlas sacado del consumidor: antes vivian dentro de
 * un metodo anotado con @KafkaListener, envueltas en un try que se tragaba los errores.
 */
@ExtendWith(MockitoExtension.class)
class IngestaDeEventosServiceTest {

    @Mock
    private ReservationStatRepository repository;

    @InjectMocks
    private IngestaDeEventosService service;

    @Nested
    @DisplayName("Idempotencia")
    class Idempotencia {

        @Test
        @DisplayName("Un evento nuevo se registra")
        void registraLoNuevo() {
            when(repository.existsByEventId(anyString())).thenReturn(false);

            assertThat(service.registrar(evento("reservation.created"))).isTrue();
            verify(repository).save(any(ReservationStat.class));
        }

        @Test
        @DisplayName("Un evento ya visto no se cuenta dos veces")
        void descartaElDuplicado() {
            when(repository.existsByEventId("evt-1")).thenReturn(true);

            assertThat(service.registrar(evento("reservation.created"))).isFalse();
            verify(repository, never()).save(any());
        }
    }

    @Nested
    @DisplayName("El mapeo")
    class Mapeo {

        @Test
        @DisplayName("Guarda el tipo del contrato, no el nombre de un enum")
        void guardaElTipoDelContrato() {
            when(repository.existsByEventId(anyString())).thenReturn(false);

            service.registrar(evento("reservation.in_stay"));

            ReservationStat guardado = capturado();
            assertThat(guardado.getEventType()).isEqualTo("reservation.in_stay");
            assertThat(guardado.getReservationId()).isEqualTo(42L);
            assertThat(guardado.getUnitId()).isEqualTo(7L);
        }

        @Test
        @DisplayName("Fecha el registro con el momento del hecho, no con el del consumo")
        void fechaConElMomentoDelHecho() {
            when(repository.existsByEventId(anyString())).thenReturn(false);

            service.registrar(evento("reservation.created"));

            LocalDateTime esperado = LocalDateTime.ofInstant(
                    Instant.parse("2026-11-17T12:00:00Z"), ZoneId.systemDefault());
            assertThat(capturado().getOccurredAt()).isEqualTo(esperado);
        }
    }

    @Nested
    @DisplayName("Lo que no se puede agregar")
    class Validacion {

        @Test
        @DisplayName("Un evento vacío se rechaza")
        void eventoVacio() {
            assertThatThrownBy(() -> service.registrar(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Sin eventId se rechaza, porque no hay idempotencia posible")
        void sinEventId() {
            ReservationEvent sinId = new ReservationEvent("reservation.created", "  ",
                    Instant.now(), "t", "c", "origen", actor(), payload());

            assertThatThrownBy(() -> service.registrar(sinId))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("eventId");
        }

        @Test
        @DisplayName("Sin reserva se rechaza")
        void sinReserva() {
            ReservationEvent sinReserva = new ReservationEvent("reservation.created", "evt-9",
                    Instant.now(), "t", "c", "origen", actor(),
                    new ReservationPayload(null, null, null, null, null, null, null,
                            "CREADA", null, null, null, null, null));

            assertThatThrownBy(() -> service.registrar(sinReserva))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Sin timestamp se rechaza, en vez de fecharlo con la hora del consumo")
        void sinTimestamp() {
            ReservationEvent sinMomento = new ReservationEvent("reservation.created", "evt-9",
                    null, "t", "c", "origen", actor(), payload());

            assertThatThrownBy(() -> service.registrar(sinMomento))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("timestamp");
            verify(repository, never()).save(any());
        }
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    private ReservationStat capturado() {
        ArgumentCaptor<ReservationStat> captor = ArgumentCaptor.forClass(ReservationStat.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }

    private static Actor actor() {
        return new Actor("sub-operador", "Operador", "staff");
    }

    private static ReservationPayload payload() {
        return new ReservationPayload(42L, 7L, "sub-huesped", "h@andesstay.cl",
                LocalDate.of(2026, 3, 10), LocalDate.of(2026, 3, 13), 2,
                "CREADA", null, null, null, null, null);
    }

    private static ReservationEvent evento(String tipo) {
        return new ReservationEvent(tipo, "evt-1", Instant.parse("2026-11-17T12:00:00Z"),
                "traza-1", "corr-1", "ms-andesstay-reservations", actor(), payload());
    }
}
