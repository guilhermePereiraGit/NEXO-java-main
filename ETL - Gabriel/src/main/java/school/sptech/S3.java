package school.sptech;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.cglib.core.Local;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class S3 {

    private DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration();
    private JdbcTemplate template = databaseConfiguration.getTemplate();

    /* * Método removido, pois a lógica de data será aplicada diretamente no buscarDadosJira
     * public LocalDate obterUltimoOuAtualDomingo() { ... }
     */

    public List<ArquivoCsvJira> buscarDadosJira(Integer idEmpresa) {

        // Instanciando um objero da classe Mapper
        Mapper map = new Mapper();

        // Pegando o dia atual (limite superior da busca)
        LocalDate hoje = LocalDate.now();

        // Pegando sete dias atrás (início da busca, para cobrir 8 dias no total: 7 dias + hoje)
        LocalDate oitoDiasAtras = hoje.minusDays(8);

        // Criando um formatador de data
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // Criando uma conexão com o S3
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

        // Informando qual o bucket que queremos buscar os dados
        String bucketName = "trusted-projeto-individual-nexo";

        // Criando uma lista de objetos da classe ArquivoCsvJira
        List<ArquivoCsvJira> dadosJiraDaEmpresa = new ArrayList<>();

        // Buscando todos os macs da empresa
        String sql = """
        SELECT t.numMac
        FROM totem t
        JOIN modelo m ON m.idModelo = t.fkModelo
        JOIN empresa e on m.fkEmpresa = e.idEmpresa
        WHERE m.fkEmpresa = ?
    """;

        // Adicionando eles em uma lista de String
        List<String> totens = template.queryForList(sql, String.class, idEmpresa);

        // Para cada totem da lista...
        for (String totem : totens) {

            //Para cada dia dentre 8 dias atrás e o dia atual...
            // O loop vai de oitoDiasAtras até hoje (inclusive).
            for (LocalDate d = oitoDiasAtras; !d.isAfter(hoje); d = d.plusDays(1)) {

                // Formatando a data
                String dia = d.format(fmt);

                // Informando qual o prefixo do diretório que está o arquivo
                String prefixo = idEmpresa + "/" + totem + "/" + dia + "/";

                try {
                    // Listando os arquivos desse dia
                    ObjectListing listing = s3.listObjects(bucketName, prefixo);
                    List<S3ObjectSummary> arquivos = listing.getObjectSummaries();

                    // Verificando se há algum arquivo no dia
                    if (arquivos.isEmpty()) {
                        System.out.printf("Nenhum arquivo encontrado para o dia %s no totem %s%n", dia, totem);
                        continue;
                    }

                    // Pegando o único arquivo do dia
                    Optional<S3ObjectSummary> arquivoValido = arquivos.stream()
                            .filter(a -> !a.getKey().endsWith("/"))
                            .findFirst();

                    if (arquivoValido.isEmpty()) {
                        System.out.printf("Nenhum arquivo CSV encontrado para o dia %s no totem %s%n", dia, totem);
                        continue;
                    }

                    String key = arquivoValido.get().getKey();

                    // Baixando o arquivo
                    S3Object s3Object = s3.getObject(bucketName, key);

                    // Verificando se o objeto retornado está vazio
                    if (s3Object.getObjectMetadata().getContentLength() == 0) {
                        System.out.println("Arquivo vazio: " + key);
                        continue;
                    }

                    try (InputStream s3InputStream = s3Object.getObjectContent()) {

                        // Transformando o arquivo do dia em um objeto da classe ArquivoCsvJira
                        ArquivoCsvJira csv = map.mapDadosJira(s3InputStream);

                        // Adicionando esse arquivo na lista de objetos da classe ArquivoCsvJira
                        dadosJiraDaEmpresa.add(csv);

                        System.out.println("Arquivo " + key + " processado com sucesso!");

                    } catch (Exception parseEx) {
                        System.out.println("Erro ao processar CSV " + key + ": " + parseEx.getMessage());
                    }

                } catch (Exception e) {
                    System.out.printf("Erro ao buscar arquivos do prefixo %s: %s%n", prefixo, e.getMessage());
                    continue;
                }
            }
        }
        return dadosJiraDaEmpresa;
    }

    public void enviarDadosBucket(String json, Integer idEmpresa){

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

        String bucketName = "client-projeto-individual-nexo";

        // A data na chave será a data de hoje, representando quando o ETL foi executado.
        LocalDate dataAtual = LocalDate.now();

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String dataFormatada = dataAtual.format(fmt);

        String key = idEmpresa + "/" + dataFormatada + "/etl.json";

        s3.putObject(bucketName, key, json);
    }
}