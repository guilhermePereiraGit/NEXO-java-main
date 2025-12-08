package school.sptech;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.bean.CsvToBeanBuilder;
import java.io.InputStream;
import java.io.InputStreamReader;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class Mapper {

    S3 s3 = new S3();

    public ArquivoCsvJira mapDadosJira(InputStream inputStream) throws Exception{
        return new CsvToBeanBuilder<ArquivoCsvJira>(new InputStreamReader(inputStream))
                .withType(ArquivoCsvJira.class)
                .withIgnoreLeadingWhiteSpace(true)
                .build()
                .parse()
                .get(0);
    }

    public void json(EstruturaJson estruturaJson, Integer idEmpresa) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        // *** SOLUÇÃO: Registrar o JavaTimeModule ***
        mapper.registerModule(new JavaTimeModule());

        // Desativa a escrita de datas/horas como Timestamps (que não funcionam bem com Duration)
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Mantém a formatação legível (indentação)
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        // Tenta escrever os dados
        String json = mapper.writeValueAsString(estruturaJson);

        // Envia
        s3.enviarDadosBucket(json, idEmpresa);
    }
}
