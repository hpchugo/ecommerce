package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.commons.lang3.RandomStringUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public void destroy() {
        orderDispatcher.close();
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp){
        try {
            String orderID = UUID.randomUUID().toString();
            String email = !req.getParameter("email").equalsIgnoreCase("") ? req.getParameter("email") : generateRandomEmail();
            BigDecimal amount = new BigDecimal(!req.getParameter("amount").equalsIgnoreCase("") ? req.getParameter("amount") : generateRandomBigDecimalFromRange(new BigDecimal(6000)).toString());
            String orderId = req.getParameter(!req.getParameter("uuid").equalsIgnoreCase("") ? req.getParameter("uuid") : UUID.randomUUID().toString());
            Order order = new Order(orderID, amount, email);
            var database = new OrdersDatabase();
            resp.setStatus(HttpServletResponse.SC_OK);
            if(database.saveNew(order)){
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()),order);
                resp.getWriter().println("New Order sent successfully!");
            }else{
                resp.getWriter().println("Old order received!");
            }

        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static String generateRandomEmail() {
        String allowedChars = "abcdefghijklmnopqrstuvwxyz" + "1234567890";
        String email;
        String temp = RandomStringUtils.random(20, allowedChars);
        email = temp.substring(0, temp.length() - 9) + "@testdata.com";
        return email;
    }

    private static BigDecimal generateRandomBigDecimalFromRange(BigDecimal max) {
        return BigDecimal.ONE.add(BigDecimal.valueOf(Math.random()).multiply(max.subtract(BigDecimal.ONE))).setScale(2, RoundingMode.UP);
    }
}
