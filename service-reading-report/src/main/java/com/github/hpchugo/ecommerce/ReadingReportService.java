package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.consumer.ConsumerService;
import com.github.hpchugo.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;

public class ReadingReportService implements ConsumerService<User> {
    public static void main(String[] args) {
        new ServiceRunner(ReadingReportService::new).start(5);
    }

    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }

    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<User>> record){
        System.out.println("------------------------------------------------------------------------");
        System.out.printf("Processing report for %s\n", record.value());
        var message = record.value();
        var user = message.getPayload();
        var target = new File(user.getReportPath());
        try {
            Path filePath = Path.of(Objects.requireNonNull(getClass().getResource("/report.txt")).toURI());
            IO.copyTo(filePath, target);
            IO.append(target, String.format("Created for %s", user.getUuid()));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        System.out.printf("File created: %s%n", target.getAbsolutePath());
        System.out.println("------------------------------------------------------------------------");
    }
}