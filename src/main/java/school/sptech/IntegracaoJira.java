package school.sptech;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

public class IntegracaoJira {
    public static void criarChamado(String summary, String description) throws IOException {
        String jiraUrl = "https://nexoadm.atlassian.net/rest/servicedeskapi/request";
        String email = "nexoadm9328@outlook.com";
        String apiToken = "ATATT3xFfGF0AjNvGjQtrKdVrlAXOvYIy_IfpUkhBMMxsEWXRiKrgN_0VaxPH5AamAxc4BFBKfmohZHn_8l5fUDB83ZOjGqBleL6ztAfOfEJsfboM2_ism62a2t_lWZ48PgoiP-KGT9Xh1saeHF8ZPa1UPIWbsgX0ItLCNZs0Y8jBV3V_hRR3Gc=AF1CC7F7";

        String auth = Base64.getEncoder().encodeToString((email + ":" + apiToken).getBytes());

        String json = "{"
                + "\"serviceDeskId\": \"2\","
                + "\"requestTypeId\": \"15\","
                + "\"requestFieldValues\": {"
                + "\"summary\": \"" + summary + "\","
                + "\"description\": \"" + description + "\""
                + "}"
                + "}";

        URL url = new URL(jiraUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + auth);
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(json.getBytes());
        }

        if (conn.getResponseCode() != 201) {
            System.out.println("Erro ao criar chamado: " + conn.getResponseCode());
        }
    }
}
