import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;
import org.apache.camel.support.processor.idempotent.FileIdempotentRepository;
import org.apache.camel.spi.IdempotentRepository;
import java.io.File;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class FileTransferRoute extends RouteBuilder {

    private static final String EXPECTED_HEADER =
            "patient_id,full_name,appointment_date,insurance_code";
    private static final Pattern DATE_PATTERN =
            Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
    private static final List<String> VALID_INSURANCE =
            Arrays.asList("IESS", "PRIVADO", "NINGUNO");

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.configure().addRoutesBuilder(new FileTransferRoute());
        main.run();
    }

    @Override
    public void configure() throws Exception {

        IdempotentRepository idempotentRepo =
                FileIdempotentRepository.fileIdempotentRepository(
                        new File("logs/processed-files.dat"), 1000);

        getContext().getRegistry().bind("fileStore", idempotentRepo);

        from("file:input?delete=true&idempotent=true&idempotentRepository=#fileStore")
                .log("Archivo detectado: ${file:name}")
                .convertBodyTo(String.class)
                .process(new CsvValidator())
                .choice()
                .when(header("valid").isEqualTo(true))
                .log("El archivo VALIDO: ${file:name}")
                .to("file:output")
                .log("Enviado a /output")
                .setHeader(Exchange.FILE_NAME, simple(
                        "${file:name.noext}_${date:now:yyyy-MM-dd_HHmmss}.csv"))
                .to("file:archive")
                .log("Archivado como ${header.CamelFileName}")
                .otherwise()
                .log("El archivo es INVALIDO: ${file:name} — Motivo: ${header.errorReason}")
                .setHeader(Exchange.FILE_NAME, simple(
                        "${file:name.noext}_${date:now:yyyy-MM-dd_HHmmss}.csv"))
                .to("file:error")
                .log("Enviado a /error como ${header.CamelFileName}")
                .end();
    }

    static class CsvValidator implements Processor {
        @Override
        public void process(Exchange exchange) {
            String body = exchange.getIn().getBody(String.class);
            String[] lines = body.split("\\r?\\n");

            if (lines.length == 0 ||
                    !lines[0].trim().equalsIgnoreCase(EXPECTED_HEADER)) {
                setInvalid(exchange,
                        "Encabezado incorrecto, esperado: " + EXPECTED_HEADER);
                return;
            }

            for (int i = 1; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty()) continue;

                String[] fields = line.split(",", -1);

                if (fields.length != 4) {
                    setInvalid(exchange,
                            "Fila " + i + ": numero de columnas incorrecto");
                    return;
                }

                for (int j = 0; j < fields.length; j++) {
                    if (fields[j].trim().isEmpty()) {
                        setInvalid(exchange,
                                "Fila " + i + ": campo vacio en columna " + (j + 1));
                        return;
                    }
                }

                if (!DATE_PATTERN.matcher(fields[2].trim()).matches()) {
                    setInvalid(exchange,
                            "Fila " + i + ": appointment_date invalida (" + fields[2] + ")");
                    return;
                }

                if (!VALID_INSURANCE.contains(fields[3].trim())) {
                    setInvalid(exchange,
                            "Fila " + i + ": insurance_code invalido (" + fields[3] + ")");
                    return;
                }
            }

            exchange.getIn().setHeader("valid", true);
        }

        private void setInvalid(Exchange exchange, String reason) {
            exchange.getIn().setHeader("valid", false);
            exchange.getIn().setHeader("errorReason", reason);
        }
    }
}