package org.demo.chapter06;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.List;

/**
 * Created by lhqz on 2017/8/5.
 */
public class AutoCompleteContactTest {

    private Jedis conn;
    private AutoCompleteContact contact;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.110");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        contact = new AutoCompleteContact();
        contact.setConn(conn);
    }

    @Test
    public void testAddUpdateContact() {
        System.out.println("\n----- testAddUpdateContact -----");

        System.out.println("Let's add a few contacts...");
        for (int i = 0; i < 10; i++) {
            contact.addUpdateContact("user", "contact-" + ((int) Math.floor(i / 3)) + '-' + i);
        }
        System.out.println("Current recently contacted contacts");
        List<String> contacts = conn.lrange(contact.getContactKey("user"), 0, -1);
        for (String contact : contacts) {
            System.out.println("\t" + contact);
        }
        assert contacts.size() == 10;

        System.out.println("Let's pull one of the older ones up to the front");
        contact.addUpdateContact("user", "contact-1-4");
        contacts = conn.lrange(contact.getContactKey("user"), 0, 2);
        System.out.println("New top-3 contacts:");
        for (String contact : contacts) {
            System.out.println("\t" + contact);
        }
        assert "contact-1-4".equals(contacts.get(0));
        System.out.println();

        System.out.println("Let's remove a contact...");
        contact.removeContact("user", "contact-2-6");
        contacts = conn.lrange(contact.getContactKey("user"), 0, -1);
        for (String contact : contacts) {
            System.out.println("\t" + contact);
        }
        assert contacts.size() == 9;
        System.out.println();

        System.out.println("Add let's finally autocomplete on ");
        List<String> all = conn.lrange(contact.getContactKey("user"), 0, -1);
        contacts = contact.fetchAutoCompleteList("user", "c");
        assert all.equals(contacts);
        List<String> equiv = Lists.newArrayList();
        for (String contact : all) {
            if (contact.startsWith("contact-2-")) {
                equiv.add(contact);
            }
        }
        contacts = contact.fetchAutoCompleteList("user", "contact-2-");
        Collections.sort(equiv);
        Collections.sort(contacts);
        assert equiv.equals(contacts);
    }

    @After
    public void destroy() {
        conn.flushDB();
    }

}