package sptech.nexo;

import org.springframework.jdbc.core.JdbcTemplate;

public class Empresas {
    private JdbcTemplate conexaoBanco;

    //Construtor para Inicializar a Conexão
    public Empresas(Connection conexaoBanco) {
        this.conexaoBanco = new JdbcTemplate(conexaoBanco.getDataSource());
    }

    //Métodos
    public Integer pegarTotalEmpresas(){
        String comandoSql = "select count(*) from empresa";
        Integer totalEmpresas = conexaoBanco.queryForObject(comandoSql,Integer.class);
        return totalEmpresas;
        //Poderia ser feito utilizando a Connection, mas como não irei aplicar um filtro específico
        //É possível executar direto da biblioteca JdbcTemplate assim como explicado em aula
    }
}
