    package sptech.nexo;

    import org.springframework.jdbc.core.RowMapper;
    import java.sql.ResultSet;
    import java.sql.SQLException;

    public class TotemMapper implements RowMapper<Totem>{
        @Override
        public Totem mapRow(ResultSet rs, int rowNum) throws SQLException {
            //Preenche o Totem
            Totem totem = new Totem();
            totem.setIdTotem(rs.getInt("idTotem"));
            totem.setNumMac(rs.getString("numMac"));

            //Preenche o Modelo para o Totem
            Modelo modelo = new Modelo();
            modelo.setIdModelo(rs.getInt("idModelo"));
            modelo.setNome(rs.getString("nomeModelo"));
            totem.setModelo(modelo);

            //Preenche Região para Totem
            Regiao regiao = new Regiao();
            regiao.setNomeRegiao(rs.getString("nomeRegiao"));
            regiao.setSigla(rs.getString("sigla"));
            totem.setRegiao(regiao);
            return totem;
        }
    }
