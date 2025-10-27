package school.sptech;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TotemRowMapper implements RowMapper<Totem> {

    @Override
    public Totem mapRow(ResultSet rs, int rowNum) throws SQLException {
        Totem totem = new Totem();
        totem.setIdTotem(rs.getInt("idTotem"));
        totem.setNumMac(rs.getString("numMac"));
        totem.setStatus(rs.getString("status"));
        totem.setFkEndereco(rs.getInt("fkEndereco"));

        // Preenche o Modelo
        Modelo modelo = new Modelo();
        modelo.setIdModelo(rs.getInt("idModelo"));
        modelo.setNome(rs.getString("nome"));
        modelo.setDescricaoArq(rs.getString("descricao_arq"));
        modelo.setStatus(rs.getString("status"));
        modelo.setFkEmpresa(rs.getInt("fkEmpresa"));

        totem.setModelo(modelo);

        return totem;
    }
}
