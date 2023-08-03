package com.tickerfit.domain.utility;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Persistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class PrefixedEntitySequenceIdGeneratorTest {
    private EntityManager entityManager;

    @Before
    public void init() throws IOException {
        Properties properties = new Properties();
        properties.load(PrefixedEntitySequenceIdGenerator.class.getClassLoader().getResourceAsStream("database.properties"));
        entityManager = Persistence.createEntityManagerFactory("entityManager", properties).createEntityManager();
        deleteAll();
    }

    @After
    public void after() {
        deleteAll();
    }

    private void deleteAll() {
        entityManager.getTransaction().begin();
        entityManager.createQuery("DELETE from Invoice").executeUpdate();
        entityManager.getTransaction().commit();
    }

    @Test
    public void persistOneInvoice() {
        Invoice invoice = persistNewInvoice();
        assertNotNull(invoice.getId());
        assertEquals("INV-00001", invoice.getId());
    }

    @Test
    public void persist20Invoices() {
        int originalCount = entityManager.createQuery("SELECT count(i) from Invoice i").getFirstResult();
        for (int expectedNumber=originalCount+1; expectedNumber<originalCount+20; expectedNumber++)
        {
            Invoice invoice = persistNewInvoice();
            assertEquals("INV-"+String.format("%05d",expectedNumber), invoice.getId());
        }
    }

    private Invoice persistNewInvoice() {
        entityManager.getTransaction().begin();
        Invoice invoice = new Invoice();
        entityManager.persist(invoice);
        entityManager.getTransaction().commit();
        return invoice;
    }
}
