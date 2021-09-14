package com.example.batch;

import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class OrderFieldMapper implements org.springframework.batch.item.file.mapping.FieldSetMapper<Order> {
    @Override
    public Order mapFieldSet(FieldSet fieldSet) throws BindException {
        Order order = new Order();
        order.setOrderId(fieldSet.readLong("order_id"));
        order.setCost(fieldSet.readBigDecimal("cost"));
        order.setEmail(fieldSet.readString("email"));
        order.setFirstName(fieldSet.readString("first_name"));
        order.setLastName(fieldSet.readString("last_name"));
        order.setItemId(fieldSet.readString("item_id"));
        order.setShipDate(fieldSet.readDate("ship_date"));
        order.setItemName(fieldSet.readString("item_name"));
        return order;
    }
}
