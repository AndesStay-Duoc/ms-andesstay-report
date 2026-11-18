package cl.andesstay.report.messaging.events;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Los datos de la reserva que llegan dentro del envelope, tal como los publica
 * ms-andesstay-reservations. Copia del record del productor; ver el comentario de Actor.
 *
 * Los campos opcionales llegan en null cuando no aplican: estadoAnterior no existe en la creacion,
 * motivo solo viene en la cancelacion y duracionEstadiaHoras solo en el checkout.
 */
public record ReservationPayload(
        Long reservationId,
        Long unitId,
        String guestId,
        String guestEmail,
        LocalDate fechaEntrada,
        LocalDate fechaSalida,
        Integer huespedes,
        String estado,
        String estadoAnterior,
        String motivo,
        BigDecimal tarifaTotal,
        String moneda,
        Long duracionEstadiaHoras) {
}
