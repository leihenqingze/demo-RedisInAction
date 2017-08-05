package org.demo.chapter06;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Set;

/**
 * Created by lhqz on 2017/8/5.
 */
public class AutoCompleteAddressBookTest {

    private Jedis conn;
    private AutoCompleteAddressBook addressBook;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.110");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        addressBook = new AutoCompleteAddressBook();
        addressBook.setConn(conn);
    }

    @Test
    public void testAddressBookAutocomplete() {
        System.out.println("\n----- testAddressBookAutocomplete -----");
        System.out.println("the start/end range of 'abc' is: "
                + Arrays.toString(addressBook.findPrefixRange("abc")));
        System.out.println();

        System.out.println("Let's add a few people to the guild");
        for (String name : new String[]{"jeff","jenny","jack","jennifer"}){
            addressBook.joinGuild("test",name);
        }
        System.out.println();
        System.out.println("now let's try to find users with names starting with 'je':");
        Set<String> r = addressBook.autocompleteOnPrefix("test","je");
        System.out.println(r);
        assert r.size() == 3;

        System.out.println("jeff just left to join a different guild...");
        addressBook.leaveGuild("test","jeff");
        r = addressBook.autocompleteOnPrefix("test","je");
        System.out.println(r);
        assert r.size() == 2;
    }

    @After
    public void destroy() {
        conn.flushDB();
    }

}