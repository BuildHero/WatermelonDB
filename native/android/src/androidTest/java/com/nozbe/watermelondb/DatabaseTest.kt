package com.nozbe.watermelondb

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import androidx.test.ext.junit.runners.AndroidJUnit4
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class DatabaseTest {
    private val context: Context = ApplicationProvider.getApplicationContext()
    private var db: Database? = null

    private fun makeDatabase(): Database {
        val database = Database("wmdb-test-${System.nanoTime()}", context)
        db = database
        return database
    }

    @After
    fun tearDown() {
        db?.close()
        db = null
    }

    @Test
    fun readOnlyQueryRoutesToReader() {
        val database = makeDatabase()
        assertTrue(database._test_isReadOnlyQuery("select 1"))
        assertEquals("reader", database._test_readDatabaseIdentity("select 1"))
    }

    @Test
    fun writeQueryRoutesToWriter() {
        val database = makeDatabase()
        assertFalse(database._test_isReadOnlyQuery("update foo set bar = 1"))
        assertEquals("writer", database._test_readDatabaseIdentity("update foo set bar = 1"))
    }

    @Test
    fun transactionForcesWriter() {
        val database = makeDatabase()
        database.transaction {
            assertEquals("writer", database._test_readDatabaseIdentity("select 1"))
        }
    }

    @Test
    fun readerIsQueryOnly() {
        val database = makeDatabase()
        assertEquals(1, database._test_readerQueryOnlyValue())
    }

    @Test
    fun readOnlyPrefixes() {
        val database = makeDatabase()
        assertTrue(database._test_isReadOnlyQuery("with cte as (select 1) select * from cte"))
        assertTrue(database._test_isReadOnlyQuery("explain select 1"))
    }
}
