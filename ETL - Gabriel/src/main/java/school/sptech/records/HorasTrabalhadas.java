package school.sptech.records;

import java.time.Duration;

public record HorasTrabalhadas(
        int empresaId,
        String regiao,
        Duration duracao
) {}