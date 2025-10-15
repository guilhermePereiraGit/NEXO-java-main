package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;
import java.time.LocalDateTime;

public class ServicoAlerta {
    private JdbcTemplate jdbcTemplate;

    public ServicoAlerta(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void registrarAlertaCPU(String idTotem, Double valorCPU, Double limite) {
        if (valorCPU > limite) {
            registrarAlerta(idTotem,
                    String.format("Uso de CPU crítico: %.1f%%", valorCPU),
                    "CPU", valorCPU, limite);
        }
    }

    public void registrarAlertaRAM(String idTotem, Double valorRAM, Double limite) {
        if (valorRAM > limite) {
            registrarAlerta(idTotem,
                    String.format("Uso de RAM crítico: %.1f%%", valorRAM),
                    "RAM", valorRAM, limite);
        }
    }

    public void registrarAlertaDisco(String idTotem, Double valorDisco, Double limite) {
        if (valorDisco > limite) {
            registrarAlerta(idTotem,
                    String.format("Uso de Disco crítico: %.1f%%", valorDisco),
                    "DISCO", valorDisco, limite);
        }
    }

    private void registrarAlerta(String idTotem, String mensagem, String tipo, Double valorMedido, Double limite) {
        String sql = "INSERT INTO alerta (descricao, dataHora, fkTotem, tipo) VALUES (?, ?, ?, ?)";

        try {
            jdbcTemplate.update(sql, mensagem, LocalDateTime.now(), idTotem, tipo);
            System.out.println("ALERTA REGISTRADO NO BANCO: " + mensagem);
        } catch (Exception erro) {
            System.out.println("ALERTA (apenas console): " + mensagem);
            System.out.println("Erro BD: " + erro.getMessage());
        }
    }
}