package school.sptech;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import io.github.cdimascio.dotenv.Dotenv;

public class IntegracaoJira {

    public static String getUltimaDataTicket(String mac) throws IOException {
        Dotenv dotenv = Dotenv.load();
        String baseUrl = dotenv.get("JIRA_URL_LISTAGEM");
        String email = dotenv.get("JIRA_EMAIL");
        String apiToken = dotenv.get("JIRA_API_TOKEN");
        String projectKey = dotenv.get("JIRA_PROJECT_KEY");

        String auth = Base64.getEncoder().encodeToString((email + ":" + apiToken).getBytes());

        String jql = "project=" + projectKey + " AND summary~\"" + mac + "\" ORDER BY created DESC";
        String encodedJql = java.net.URLEncoder.encode(jql, StandardCharsets.UTF_8);

        URL url = new URL(baseUrl + "/rest/api/3/search?jql=" + encodedJql + "&maxResults=1&fields=created");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Basic " + auth);
        conn.setRequestProperty("Accept", "application/json");

        if (conn.getResponseCode() != 200) return null;

        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) response.append(line);
        reader.close();

        Pattern pattern = Pattern.compile("\"created\":\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(response.toString());
        if (matcher.find()) return matcher.group(1);
        return null;
    }

    public static boolean existeChamadoAbertoParaMac(String mac) throws IOException {
        Dotenv dotenv = Dotenv.load();
        String baseUrl = dotenv.get("JIRA_URL_LISTAGEM");
        String email = dotenv.get("JIRA_EMAIL");
        String apiToken = dotenv.get("JIRA_API_TOKEN");
        String projectKey = dotenv.get("JIRA_PROJECT_KEY");

        String auth = Base64.getEncoder().encodeToString((email + ":" + apiToken).getBytes());

        String jql = "project=" + projectKey + " AND summary~\"" + mac + "\" AND statusCategory != Done";
        String encodedJql = java.net.URLEncoder.encode(jql, StandardCharsets.UTF_8);

        URL url = new URL(baseUrl + "/rest/api/3/search?jql=" + encodedJql + "&maxResults=1&fields=created");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Basic " + auth);
        conn.setRequestProperty("Accept", "application/json");

        if (conn.getResponseCode() != 200) return false;

        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) response.append(line);
        reader.close();

        return response.toString().matches(".*\"total\":\\s*[1-9].*");
    }

    public static void criarChamado(String summary, String description) throws IOException {
        Dotenv dotenv = Dotenv.load();
        String jiraUrl = dotenv.get("JIRA_URL");
        String email = dotenv.get("JIRA_EMAIL");
        String apiToken = dotenv.get("JIRA_API_TOKEN");

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
            os.write(json.getBytes(StandardCharsets.UTF_8));
        }

        if (conn.getResponseCode() != 201) {
            System.out.println("Erro ao criar chamado: " + conn.getResponseCode());
        }
    }
}
