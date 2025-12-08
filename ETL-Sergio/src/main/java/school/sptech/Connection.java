package school.sptech;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

public class Connection {

    // DataSource único para o projeto inteiro
    private static DataSource dataSource;

    // Cria (se ainda não existir) e devolve o DataSource
    public static DataSource getDataSource() {
        if (dataSource == null) {

            BasicDataSource ds = new BasicDataSource();
            // Driver do MySQL
            ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
            // URL do seu banco

            ds.setUrl("jdbc:mysql://174.129.108.106:3306/NEXO_DB");

            // Usuário e senha (os que você usa no Workbench)
            ds.setUsername("root");
            ds.setPassword("urubu100");
            dataSource = ds;
        }

        return dataSource;
    }

    // Abre uma conexão de banco
    public static java.sql.Connection getConnection() {
        try {
            return getDataSource().getConnection();
        } catch (SQLException e) {
            System.out.println("[CONNECTION] Erro ao abrir conexão: " + e.getMessage());
            return null;
        }
    }
}