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

    // -- referencesTemporaryTable detection --

    @Test
    fun detectsTempDotPrefix() {
        val database = makeDatabase()
        assertTrue(database._test_referencesTemporaryTable("SELECT * FROM temp.product_search"))
        assertTrue(database._test_referencesTemporaryTable("select * from TEMP.product_search"))
        assertTrue(database._test_referencesTemporaryTable("CREATE VIRTUAL TABLE IF NOT EXISTS temp.product_search USING fts5(id)"))
        assertTrue(database._test_referencesTemporaryTable("INSERT OR REPLACE INTO temp.product_search(rowid, id) SELECT rowid, id FROM products"))
    }

    @Test
    fun detectsSqliteTempMaster() {
        val database = makeDatabase()
        assertTrue(database._test_referencesTemporaryTable("SELECT name FROM sqlite_temp_master WHERE type='table' AND name='product_search'"))
        assertTrue(database._test_referencesTemporaryTable("SELECT name FROM SQLITE_TEMP_MASTER WHERE type='table'"))
    }

    @Test
    fun doesNotFalsePositiveOnRegularQueries() {
        val database = makeDatabase()
        assertFalse(database._test_referencesTemporaryTable("SELECT * FROM products"))
        assertFalse(database._test_referencesTemporaryTable("SELECT * FROM products WHERE name = 'temporary'"))
        assertFalse(database._test_referencesTemporaryTable("WITH cte AS (SELECT 1) SELECT * FROM cte"))
        assertFalse(database._test_referencesTemporaryTable("UPDATE products SET name = 'test'"))
    }

    // -- readDatabase routing with temp tables --

    @Test
    fun tempDotSelectRoutesToWriter() {
        val database = makeDatabase()
        assertEquals("writer", database._test_readDatabaseIdentity(
            "SELECT * FROM temp.product_search('test')"
        ))
    }

    @Test
    fun withCteTempRoutesToWriter() {
        val database = makeDatabase()
        // WITH queries referencing temp tables should go to writer
        assertEquals("writer", database._test_readDatabaseIdentity(
            """
            WITH filtered_search AS (
                SELECT id FROM temp.product_search(?)
            )
            SELECT * FROM filtered_search
            """.trimIndent()
        ))
    }

    @Test
    fun sqliteTempMasterRoutesToWriter() {
        val database = makeDatabase()
        assertEquals("writer", database._test_readDatabaseIdentity(
            "SELECT name FROM sqlite_temp_master WHERE type='table' AND name='product_search'"
        ))
    }

    @Test
    fun regularSelectStillRoutesToReader() {
        val database = makeDatabase()
        assertEquals("reader", database._test_readDatabaseIdentity("SELECT * FROM products"))
        assertEquals("reader", database._test_readDatabaseIdentity(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ))
    }

    @Test
    fun tempSubstringInStringLiteralStillRoutesToWriter() {
        val database = makeDatabase()
        // Known false positive: "temp." appearing in a string literal or as a table alias
        // routes to writer. This is a safe fallback (just reduces read parallelism).
        assertEquals("writer", database._test_readDatabaseIdentity(
            "SELECT * FROM products WHERE name = 'temp.file'"
        ))
    }
}
