package sptech.nexo;

import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TotemMapper implements RowMapper<Totem>{
    @Override
    public Totem mapRow(ResultSet rs, int rowNum) throws SQLException {
        Totem totem = new Totem();
        totem.setIdTotem(rs.getInt("idTotem"));
        totem.setNumMac(rs.getString("numMac"));
        totem.setNomeModelo(rs.getString("nomeModelo"));
        totem.setNomeRegiao(rs.getString("nomeRegiao"));
        totem.setSiglaRegiao(rs.getString("sigla"));
        return totem;
    }
}
