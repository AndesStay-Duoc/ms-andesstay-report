package cl.andesstay.report.streams;

import cl.andesstay.report.messaging.events.ReservationEvent;

import java.time.Duration;
import java.time.Instant;

/**
 * El estado acumulado de una reserva, tal como lo va construyendo Kafka Streams.
 *
 * ES EL UNICO ESTADO QUE GUARDA LA TOPOLOGIA y resuelve dos de los tres KPIs del caso: la
 * ocupacion activa sale de contar cuantas reservas estan EN_ESTADIA, y el tiempo de ciclo de
 * restar el instante de creacion al de cierre. Tenerlos en un solo almacen y no en dos evita
 * mantener dos vistas del mismo hecho que podrian discrepar.
 *
 * POR QUE SE PUEDE ACUMULAR ASI. Los eventos de una reserva llegan todos a la misma particion,
 * porque la clave es el reservationId, y dentro de una particion Kafka garantiza el orden. Sin esa
 * garantia, un CHECKOUT consumido antes que su creacion daria un tiempo de ciclo negativo.
 *
 * Es inmutable: cada evento produce un ciclo nuevo. Kafka Streams reutiliza el acumulador entre
 * llamadas, y mutarlo en el sitio da resultados impredecibles tras un reequilibrio.
 *
 * @param reservationId a que reserva corresponde
 * @param unitId        unidad implicada, para cruzar con el catalogo
 * @param estado        ultimo estado conocido
 * @param creadaEn      instante del evento de creacion
 * @param cerradaEn     instante del checkout o de la cancelacion, o null si sigue abierta
 */
public record CicloDeReserva(
        Long reservationId,
        Long unitId,
        String estado,
        Instant creadaEn,
        Instant cerradaEn) {

    public static final String EN_ESTADIA = "EN_ESTADIA";
    public static final String CHECKOUT = "CHECKOUT";
    public static final String CANCELADA = "CANCELADA";

    /** El acumulador vacio con el que empieza cada reserva. */
    public static CicloDeReserva inicial() {
        return new CicloDeReserva(null, null, null, null, null);
    }

    /**
     * Incorpora un evento al ciclo.
     *
     * creadaEn solo se fija si todavia no lo estaba: si el tema se relee desde el principio o
     * llega un duplicado, el instante original no se pisa. cerradaEn se fija al llegar a un estado
     * terminal, que son los dos que cierran la maquina de estados: CHECKOUT y CANCELADA.
     */
    public CicloDeReserva con(ReservationEvent evento) {
        var p = evento.payload();
        String nuevoEstado = p.estado();

        Instant creacion = creadaEn != null ? creadaEn
                : (p.estadoAnterior() == null ? evento.timestamp() : null);

        Instant cierre = esTerminal(nuevoEstado) ? evento.timestamp() : cerradaEn;

        return new CicloDeReserva(
                p.reservationId(),
                p.unitId() != null ? p.unitId() : unitId,
                nuevoEstado,
                creacion,
                cierre);
    }

    public boolean estaEnEstadia() {
        return EN_ESTADIA.equals(estado);
    }

    public boolean estaCerrada() {
        return cerradaEn != null;
    }

    /**
     * El tiempo de ciclo: cuanto vivio la reserva desde que se creo hasta que se cerro.
     *
     * Es la medida que pide el caso y se calcula con los instantes reales de los eventos, no con
     * las fechas prometidas en la reserva. Devuelve null mientras la reserva siga abierta o si se
     * perdio el evento de creacion, que es preferible a devolver un cero que el panel promediaria
     * como si fuera un dato.
     */
    public Long minutosDeCiclo() {
        if (creadaEn == null || cerradaEn == null) {
            return null;
        }
        return Duration.between(creadaEn, cerradaEn).toMinutes();
    }

    private static boolean esTerminal(String estado) {
        return CHECKOUT.equals(estado) || CANCELADA.equals(estado);
    }
}
