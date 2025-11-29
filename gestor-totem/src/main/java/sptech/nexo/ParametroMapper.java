package sptech.nexo;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ParametroMapper implements RowMapper<Parametro> {
    @Override
    public Parametro mapRow(ResultSet rs, int rowNum) throws SQLException {
        Parametro p = new Parametro();
        p.setIdParametro(rs.getInt("idParametro"));
        p.setLimiteMax(rs.getInt("limiteMax"));

        TipoParametro tp = new TipoParametro();
        tp.setIdTipoParametro(rs.getInt("idTipoParametro"));
        tp.setComponente(rs.getString("componente"));
        tp.setStatus(rs.getString("status"));

        p.setTipoParametro(tp);
        return p;
    }
}
