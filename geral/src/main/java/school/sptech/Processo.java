package school.sptech;

import java.util.ArrayList;
import java.util.List;

public class Processo {
    private List<String> processosJson = new ArrayList<>();

    void adicionarProcesso(String nome, Double cpu, Double ram) {
        String procJson = "{\"nome\":\"" + nome + "\",\"cpu\":" +
                Math.round(cpu * 10.0) / 10.0 + ",\"ram\":" +
                Math.round(ram * 10.0) / 10.0 + "}";

        if (processosJson.size() == 5) {
            processosJson.clear();
        }

        processosJson.add(procJson);
    }

    String obterJson() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < processosJson.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(processosJson.get(i));
        }
        return sb.toString();
    }
}
