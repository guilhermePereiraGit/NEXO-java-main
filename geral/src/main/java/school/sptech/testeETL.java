package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;

public class testeETL {
    public static void main(String[] args) {
            ETL etl = new ETL();
            JdbcTemplate jdbcTemplate = new JdbcTemplate();
            // Chamar o método
            etl.dadosParaClient("33", "271371670400310", "2025-11-19", jdbcTemplate);

            // Verificar se arquivo foi criado no S3
            // Baixar e validar JSON gerado
        }

}
