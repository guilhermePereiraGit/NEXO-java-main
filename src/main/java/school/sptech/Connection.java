package school.sptech;

import org.apache.commons.dbcp2.BasicDataSource;

public class Connection {
    private BasicDataSource dataSource;

    public Connection() {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://174.129.108.106:3306/NEXO_DB");
        dataSource.setUsername("root");
        dataSource.setPassword("urubu100");
    }

    public BasicDataSource getDataSource(){
        return dataSource;
    }
}