package school.sptech;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

public class DatabaseConfiguration {

    private JdbcTemplate template;

    public DatabaseConfiguration() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://23.23.217.159/NEXO_DB");
        dataSource.setUsername("root");
        dataSource.setPassword("urubu100");
        this.template = new JdbcTemplate(dataSource);
    }

    public JdbcTemplate getTemplate() {
        return template;
    }
}