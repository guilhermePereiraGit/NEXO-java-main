package school.sptech;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ModeloRowMapper implements RowMapper<Modelo> {
    public Modelo mapRow(ResultSet rs, int rowNum) throws SQLException {
        Modelo modelo = new Modelo();
        modelo.setIdModelo(rs.getInt("idModelo"));
        modelo.setNome(rs.getString("nome"));
        modelo.setDescricaoArq(rs.getString("descricao_arq"));
        modelo.setStatus(rs.getString("status"));
        modelo.setFkEmpresa(rs.getInt("fkEmpresa"));

        return modelo;
    }
}
