package school.sptech;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ModeloRowMapper implements RowMapper<Modelo> {
    public Modelo mapRow(ResultSet rs, int rowNum) throws SQLException {
        Modelo modelo = new Modelo();
        modelo.setIdModelo(rs.getInt("idModelo"));
        modelo.setNome(rs.getString("modeloNome"));
        modelo.setCriador(rs.getString("criador"));
        modelo.setTipo(rs.getString("tipo"));
        modelo.setDescricaoArquitetura(rs.getString("descricaoArquitetura"));
        modelo.setStatus(rs.getString("statusModelo"));
        modelo.setFkEmpresa(rs.getInt("fkEmpresa"));

        return modelo;
    }
}
