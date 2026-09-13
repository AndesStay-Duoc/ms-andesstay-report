package cl.andesstay.report.controller;

import cl.andesstay.report.repository.ReservationStatRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/report")
@RequiredArgsConstructor
public class ReportController {

    private final ReservationStatRepository repository;

    /**
     * GET /api/report/kpis?range=last24h
     * Retorna: reservas por hora, ocupación activa.
     */
    @GetMapping("/kpis")
    @PreAuthorize("hasRole('Admin')")
    public ResponseEntity<Map<String, Object>> getKpis(
            @RequestParam(defaultValue = "last24h") String range) {

        LocalDateTime from = resolveFrom(range);

        List<Object[]> perHour = repository.reservationsPerHour(from);
        long active = repository.activeReservations();

        // Formatea la lista de reservas por hora
        List<Map<String, Object>> hourlyData = perHour.stream().map(row -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("hour", row[0]);
            entry.put("total", row[1]);
            return entry;
        }).toList();

        Map<String, Object> response = new HashMap<>();
        response.put("range", range);
        response.put("from", from.toString());
        response.put("activeReservations", active);
        response.put("reservationsPerHour", hourlyData);

        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/report/top-units?range=last7d
     * Unidades más demandadas en el período.
     */
    @GetMapping("/top-units")
    @PreAuthorize("hasRole('Admin')")
    public ResponseEntity<List<Map<String, Object>>> getTopUnits(
            @RequestParam(defaultValue = "last7d") String range) {

        LocalDateTime from = resolveFrom(range);
        List<Object[]> rows = repository.topUnits(from);

        List<Map<String, Object>> result = rows.stream().map(row -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("unitId", row[0]);
            entry.put("totalReservations", row[1]);
            return entry;
        }).toList();

        return ResponseEntity.ok(result);
    }

    private LocalDateTime resolveFrom(String range) {
        return switch (range) {
            case "last1h"  -> LocalDateTime.now().minusHours(1);
            case "last24h" -> LocalDateTime.now().minusHours(24);
            case "last7d"  -> LocalDateTime.now().minusDays(7);
            case "last30d" -> LocalDateTime.now().minusDays(30);
            default        -> LocalDateTime.now().minusHours(24);
        };
    }
}
