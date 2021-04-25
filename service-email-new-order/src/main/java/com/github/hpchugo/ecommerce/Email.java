package com.github.hpchugo.ecommerce;

public class Email {
    private final String subject, email;

    public Email(String subject, String email) {
        this.subject = subject;
        this.email = email;
    }

    @Override
    public String toString() {
        return "Email{" +
                "subject='" + subject + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
