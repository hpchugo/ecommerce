package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.database.LocalDatabase;

import java.io.Closeable;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {
    private final LocalDatabase database;

    OrdersDatabase()  {
        this.database = new LocalDatabase("orders_database");
        String sql = "CREATE TABLE IF NOT EXISTS Orders ( \n"
                + "	uuid varchar(200) PRIMARY KEY);";
        this.database.creationIfNotExists(sql);
    }

    public boolean saveNew(Order order) {
        if(wasProcessed(order))
            return false;
        database.update("insert into Orders(uuid) values (?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) {
        try(var result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId())) {
            return result.next();
        } catch (SQLException e) {
            return false;
        }
    }

    public void close()  {
        database.close();
    }
}
