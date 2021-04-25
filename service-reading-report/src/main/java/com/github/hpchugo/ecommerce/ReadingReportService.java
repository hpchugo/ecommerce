package com.github.hpchugo.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class ReadingReportService {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var readingReportService = new ReadingReportService();
        try(var service = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                "ECOMMERCE_USER_GENERATE_READING_REPORT",
                readingReportService::parse,
                new HashMap<>())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException, URISyntaxException {
        System.out.println("------------------------------------------------------------------------");
        System.out.printf("Processing report for %s\n", record.value());
        var message = record.value();
        var user = message.getPayload();
        var target = new File(user.getReportPath());
        Path filePath = Path.of(Objects.requireNonNull(getClass().getResource("/report.txt")).toURI());
        IO.copyTo(filePath, target);
        IO.append(target, String.format("Created for %s", user.getUuid()));
        System.out.printf("File created: %s%n", target.getAbsolutePath());
        System.out.println("------------------------------------------------------------------------");
    }

}
