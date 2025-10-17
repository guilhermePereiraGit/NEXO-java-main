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
        totem.setInstalador(rs.getString("instalador"));
        totem.setStatusTotem(rs.getBoolean("statusTotem"));
        totem.setDataInstalacao(rs.getString("dataInstalacao"));

        // Preenche o Modelo
        Modelo modelo = new Modelo();
        modelo.setIdModelo(rs.getInt("idModelo"));
        modelo.setNome(rs.getString("modeloNome"));
        modelo.setCriador(rs.getString("criador"));
        modelo.setTipo(rs.getString("tipo"));
        modelo.setDescricaoArquitetura(rs.getString("descricaoArquitetura"));
        modelo.setStatus(rs.getString("statusModelo"));
        modelo.setFkEmpresa(rs.getInt("fkEmpresa"));

        totem.setModelo(modelo);

        return totem;
    }
}
