package school.sptech;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import school.sptech.records.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Etl {

    private DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration();
    private JdbcTemplate template = databaseConfiguration.getTemplate();
    private Mapper map = new Mapper();

    public void buscarQuantidadeDeTecnicoPorRegiao() {

        String sqlEmpresas = "SELECT COUNT(*) FROM empresa";
        int qtdEmpresas = template.queryForObject(sqlEmpresas, Integer.class);

        // Faz um for com a quantidade de empresas cadastradas
        for (int empresaId = 1; empresaId <= qtdEmpresas; empresaId++) {

            // *** IMPORTANTE: 1. Instanciar um NOVO EstruturaJson para a empresa atual ***
            EstruturaJson estruturaJsonAtual = new EstruturaJson();

            // 2. Criar a lista de resultados DENTRO do loop para resetar a cada empresa
            List<TecnicosRegiao> resultadoTecnicos = new ArrayList<>();

            // ... (SQL permanece igual)
            String sql = """
                        SELECT r.nome AS regiao, COUNT(*) AS qtd
                        FROM usuario u
                        JOIN areasAtuacao aa ON aa.fkUsuario = u.idUsuario
                        JOIN regiao r ON aa.fkRegiao = r.idRegiao
                        WHERE u.fkEmpresa = ?
                        GROUP BY r.nome
                    """;

            List<Map<String, Object>> rows = template.queryForList(sql, empresaId);

            for (Map<String, Object> row : rows) {
                resultadoTecnicos.add(
                        new TecnicosRegiao(
                                empresaId,
                                row.get("regiao").toString(),
                                ((Number) row.get("qtd")).intValue()
                        )
                );
            }

            // 3. Setar APENAS os dados da empresa ATUAL
            estruturaJsonAtual.setTotalTecnicos(resultadoTecnicos);

            // 4. Passar a instância ATUAL para o próximo método
            calcularHorasIdeais(empresaId, estruturaJsonAtual);
        }
    }

    public void calcularHorasIdeais(Integer idEmpresa, EstruturaJson estruturaJsonAtual) {

        // Criamos uma lista que irá representar a lista de técnicos de cada região da empresa de id = idEmpresa
        List<TecnicosRegiao> tecnicos = estruturaJsonAtual.getTotalTecnicos();

        // Criamos uma lista de objetos do record HorasIdeais, que possuem os atributos:
        // idEmpresa, regiao e horasIdeais
        List<HorasIdeais> resultado = new ArrayList<>();

        // Para cada TecnicoRegiao da lista...
        for (TecnicosRegiao t : tecnicos) {

            // Criamos uma variável que será responsável por armazenar o resultado
            // da conta de calcular as horas ideais para cada região
            int horas = t.qtdTecnicos() * 42;

            resultado.add(
                    new HorasIdeais(
                            t.empresaId(),
                            t.regiao(),
                            horas
                    )
            );
        }

        // Setamos essa lista de quantidade de horas ideais à variável horasIdeais do record
        // EstruturaJson
        estruturaJsonAtual.setHorasIdeais(resultado);

        // Chamamos a função calcularHorasTrabalhadas passando como parâmetro o id da empresa
        calcularHorasTrabalhadas(idEmpresa, estruturaJsonAtual);
    }

    public void calcularHorasTrabalhadas(Integer idEmpresa, EstruturaJson estruturaJsonAtual) {

        // Instanciando um objeto da classe S3
        S3 s3 = new S3();

        // Criando uma lista que armazena os dados retornados do trusted
        List<ArquivoCsvJira> dados = s3.buscarDadosJira(idEmpresa);

        // Criando um formatador de dia e horário
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Buscando todas as regiões da empresa
        String sqlRegioes = """
            SELECT DISTINCT r.nome
            FROM regiao r
            JOIN areasAtuacao aa ON aa.fkRegiao = r.idRegiao
            JOIN usuario u ON u.idUsuario = aa.fkUsuario
            WHERE u.fkEmpresa = ?
        """;

        // Armazenando elas em uma lista de String
        List<String> regioes = template.queryForList(sqlRegioes, String.class, idEmpresa);

        // Criando um Map com a região e as horas trabalhadas zeradas
        Map<String, Duration> duracaoPorRegiao = new HashMap<>();
        for (String r : regioes) {
            duracaoPorRegiao.put(r, Duration.ZERO);
        }

        // Verifica se há dados antes de prosseguir com o cálculo
        if (dados.isEmpty()) {
            System.out.println("Nenhum dado do JIRA encontrado para a empresa " + idEmpresa + " no período.");
        }

        // Para cada arquivo dentro da lista...
        for (ArquivoCsvJira arquivo : dados) {

            // Pegando o id do totem
            String mac = arquivo.getId_totem();
            String regiao = null;

            // *** CORREÇÃO: VERIFICAR SE O MAC É NULO OU VAZIO ***
            if (mac == null || mac.trim().isEmpty()) {
                System.out.println("Aviso: MAC address nulo ou vazio encontrado no arquivo CSV. Pulando registro.");
                continue; // Pula para a próxima iteração do loop
            }
            // *************************************************

            // Buscando a região daquele totem
            String sqlRegiao = """
                    SELECT r.nome
                    FROM regiao r
                    JOIN endereco e ON e.fkRegiao = r.idRegiao
                    JOIN totem t ON t.fkEndereco = e.idEndereco
                    WHERE t.numMAC = ?
                """;

            try {
                // Associando a resposta da consulta para a variável regiao
                regiao = template.queryForObject(sqlRegiao, String.class, mac);
            } catch (EmptyResultDataAccessException e) {
                System.out.println("Aviso: MAC address " + mac + " não encontrado no sistema. Pulando registro.");
                continue;
            }

        }

        // Criando uma lista que irá armazenar objetos do record HorasTrabalhadas
        List<HorasTrabalhadas> resultado = new ArrayList<>();

        // Para cada região na lista de regiões...
        for (String regiao : regioes) {
            Duration duracao = duracaoPorRegiao.getOrDefault(regiao, Duration.ZERO);
            resultado.add(new HorasTrabalhadas(
                    idEmpresa,
                    regiao,
                    duracao
            ));
        }

        // Setando as horas trabalhadas na lista da classe EstruturaJson
        estruturaJsonAtual.setHorasTrabalhadas(resultado);

        // Chamando a função de calcular os funcionários recomendados passando os parâmetros
        calcularFuncionariosRecomendados(
                estruturaJsonAtual.getTotalTecnicos(),
                estruturaJsonAtual.getHorasIdeais(),
                estruturaJsonAtual.getHorasTrabalhadas(),
                idEmpresa,
                estruturaJsonAtual
        );
    }

    public void calcularFuncionariosRecomendados(List<TecnicosRegiao> tecnicos, List<HorasIdeais> horasIdeais, List<HorasTrabalhadas> horasTrabalhadas, Integer idEmpresa, EstruturaJson estruturaJsonAtual) {

        // Criamos uma lista que irá armazenar objetos do record FuncionariosRecomendados,
        // que possuem os atributos: idEmpresa, regiao e qtdRecomendada
        List<FuncionariosRecomendados> resultado = new ArrayList<>();

        // Para cada técnico na lista TecnicoRegiao...
        for (TecnicosRegiao t : tecnicos) {

            // Buscamos as HorasIdeais para a empresa e região atual
            HorasIdeais hi = horasIdeais.stream()
                    .filter(h -> h.empresaId() == t.empresaId() && h.regiao().equals(t.regiao()))
                    .findFirst().orElse(null);

            // Buscamos as HorasTrabalhadas para a empresa e região atual
            HorasTrabalhadas ht = horasTrabalhadas.stream()
                    .filter(h -> h.empresaId() == t.empresaId() && h.regiao().equals(t.regiao()))
                    .findFirst().orElse(null);

            // Se não houver HorasIdeais, pula para a próxima iteração
            if (hi == null) continue;

            // Converte a duração de HorasTrabalhadas para horas inteiras (ou 0 se for nulo)
            int horasTrabalhadasHoras = ht != null ? (int) ht.duracao().toHours() : 0;

            // Calcula o excedente de horas trabalhadas em relação às horas ideais (garante que não seja negativo)
            int excedente = Math.max(0, horasTrabalhadasHoras - hi.horasIdeais());

            // Calcula quantos funcionários podem ser reduzidos, assumindo que cada técnico
            // idealmente adiciona 49 horas (exemplo de lógica de negócio)
            int reduzir = excedente / 49;

            // Adiciona o novo objeto FuncionariosRecomendados à lista de resultados
            // A quantidade recomendada é o total de técnicos atual menos a quantidade a reduzir (garante que não seja negativo)
            resultado.add(new FuncionariosRecomendados(
                    t.empresaId(),
                    t.regiao(),
                    Math.max(0, t.qtdTecnicos() - reduzir)
            ));
        }

        // Seta a lista de quantidade de funcionários recomendados na estrutura JSON
        estruturaJsonAtual.setQtdRecomendadaDeFuncionarios(resultado);

        // Chama a função calcularComparacao passando os parâmetros
        calcularComparacao(estruturaJsonAtual.getTotalTecnicos(), idEmpresa, estruturaJsonAtual);
    }

    public void calcularComparacao(List<TecnicosRegiao> tecnicos, Integer idEmpresa, EstruturaJson estruturaJsonAtual) {

        // Criando uma lista que armazena objetos do record ComparacaoRegiao
        List<ComparacaoRegiao> resultado = new ArrayList<>();

        // Processa a lista de técnicos
        tecnicos.stream()
                // Pega todos os IDs de empresa únicos
                .map(TecnicosRegiao::empresaId)
                .distinct()
                // Para cada ID de empresa único...
                .forEach(empresaId -> {

                    // Filtra a lista de técnicos para obter apenas os técnicos da empresa atual
                    List<TecnicosRegiao> lista = tecnicos.stream()
                            .filter(t -> t.empresaId() == empresaId)
                            .toList();

                    // Calcula o total de técnicos dessa empresa
                    double total = lista.stream().mapToInt(TecnicosRegiao::qtdTecnicos).sum();

                    // Para cada região/técnico na lista filtrada...
                    for (TecnicosRegiao t : lista) {

                        // Calcula o percentual de técnicos da região em relação ao total da empresa
                        double perc = (t.qtdTecnicos() / total) * 100.0;

                        // Adiciona o objeto ComparacaoRegiao com o percentual calculado à lista de resultados
                        resultado.add(new ComparacaoRegiao(
                                empresaId,
                                t.regiao(),
                                perc
                        ));
                    }

                });

        // Seta a lista de comparação por região na estrutura JSON
        estruturaJsonAtual.setComparacao(resultado);

        try {
            // Chama o método json do Mapper para serializar a estrutura JSON e provavelmente salvá-la
            map.json(estruturaJsonAtual, idEmpresa);
        } catch (Exception e) {
            // Em caso de erro na conversão ou salvamento do JSON, exibe uma mensagem
            System.out.println("Erro ao converter os dados para json!");
        }

    }
}