package com.epimorphics.armlib.impl

import com.epimorphics.armlib.BatchRequest
import com.epimorphics.armlib.BatchStatus
import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import org.junit.Assert.*
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.ResultSet

class SqlQueueManagerTest {

    @Rule @JvmField val pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()
    private lateinit var jdbc: JdbcTemplate
    private val config = SqlQueueManager.Config(false, 500)
    private lateinit var manager: SqlQueueManager

    @Before
    fun before() {
        jdbc = JdbcTemplate(pg.embeddedPostgres.postgresDatabase)
        manager = SqlQueueManager(jdbc, config)
        manager.onStart()
    }

    private fun verify(rs: ResultSet, callback: ResultSetVerifier.() -> Unit): ResultSetVerifier {
        return ResultSetVerifier(rs).apply(callback)
    }

    private class ResultSetVerifier(
            private val rs: ResultSet
    ) {
        fun index(value: Int) {
            assertEquals(value, rs.getInt("index"))
        }

        fun key(value: String) {
            assertEquals(value, rs.getString("key"))
        }

        fun params(value: String) {
            assertEquals(value, rs.getString("params"))
        }

        fun requestUri(value: String) {
            assertEquals(value, rs.getString("requestUri"))
        }

        fun status(value: String) {
            assertEquals(value, rs.getString("status"))
        }

        fun time(value: Long) {
            assertEquals(value, rs.getLong("estimatedTime"))
        }

        fun then(callback: ResultSetVerifier.() -> Unit) {
            assertTrue(rs.next())
            callback()
        }
    }

    private fun verify(br: BatchRequest, callback: BatchRequestVerifier.() -> Unit) {
        BatchRequestVerifier(br).apply(callback)
    }

    private class BatchRequestVerifier(
            private val br: BatchRequest
    ) {
        fun key(value: String) {
            assertEquals(value, br.key)
        }

        fun params(value: String) {
            assertEquals(value, br.parameterString)
        }

        fun requestUri(value: String) {
            assertEquals(value, br.requestURI)
        }

        fun time(value: Long) {
            assertEquals(value, br.estimatedTime)
        }
    }

    @Test
    fun submit_newKey_insertsPendingEntry_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.submit(BatchRequest("test", "foo=z&bar=w"))

        jdbc.query("SELECT * FROM QueueEntry ORDER BY index") { rs ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }.then {
                index(2)
                key("test_bar_w_foo_z")
                params("bar=w&foo=z")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun submit_existingKey_pending_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun submit_existingKey_inProgress_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.nextRequest()

        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("InProgress")
                time(60000)
            }
        }
    }

    @Test
    fun submit_existingKey_complete_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.finishRequest("test_bar_y_foo_x")

        manager.submit(BatchRequest("test", "foo=x&bar=y"))

        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Completed")
                time(60000)
            }
        }
    }

    @Test
    fun submit_existingKey_failed_insertsPendingEntry_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.failRequest("test_bar_y_foo_x")

        manager.submit(BatchRequest("test", "foo=x&bar=y"))

        jdbc.query("SELECT * FROM QueueEntry ORDER BY index") { rs ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Failed")
                time(60000)
            }.then {
                index(2)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun resubmit_newKey_insertsPendingEntry_returnsStatus() {
        manager.resubmit(BatchRequest("test", "foo=x&bar=y"))
        manager.resubmit(BatchRequest("test", "foo=z&bar=w"))

        jdbc.query("SELECT * FROM QueueEntry ORDER BY index") { rs ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }.then {
                index(2)
                key("test_bar_w_foo_z")
                params("bar=w&foo=z")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun resubmit_existingKey_pending_replacesEntry_returnsStatus() {
        manager.resubmit(BatchRequest("test", "foo=x&bar=y"))
        manager.resubmit(BatchRequest("test", "foo=x&bar=y"))
        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(2)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun resubmit_existingKey_inProgress_replacesEntry_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.nextRequest()

        manager.resubmit(BatchRequest("test", "foo=x&bar=y"))
        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(2)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun resubmit_existingKey_complete_insertsPendingEntry_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.finishRequest("test_bar_y_foo_x")

        manager.resubmit(BatchRequest("test", "foo=x&bar=y"))

        jdbc.query("SELECT * FROM QueueEntry ORDER BY index") { rs ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Completed")
                time(60000)
            }.then {
                index(2)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun resubmit_existingKey_failed_insertsPendingEntry_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.failRequest("test_bar_y_foo_x")

        manager.resubmit(BatchRequest("test", "foo=x&bar=y"))

        jdbc.query("SELECT * FROM QueueEntry ORDER BY index") { rs ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Failed")
                time(60000)
            }.then {
                index(2)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun getStatus_notSubmitted_returnsUnknown() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.getStatus("test_bar_w_foo_z").apply {
            assertEquals(BatchStatus.StatusFlag.Unknown, status)
        }
    }

    @Test
    fun getStatus_submitted_pending_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.getStatus("test_bar_y_foo_x").apply {
            assertEquals(BatchStatus.StatusFlag.Pending, status)
        }
    }

    @Test
    fun getStatus_submitted_inProgress_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.nextRequest()

        manager.getStatus("test_bar_y_foo_x").apply {
            assertEquals(BatchStatus.StatusFlag.InProgress, status)
        }
    }

    @Test
    fun getStatus_submitted_complete_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.finishRequest("test_bar_y_foo_x")

        manager.getStatus("test_bar_y_foo_x").apply {
            assertEquals(BatchStatus.StatusFlag.Completed, status)
        }
    }

    @Test
    fun getStatus_submitted_failed_returnsStatus() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.failRequest("test_bar_y_foo_x")

        manager.getStatus("test_bar_y_foo_x").apply {
            assertEquals(BatchStatus.StatusFlag.Failed, status)
        }
    }

    @Test
    fun getStatus_multipleSubmitted_returnsLatest() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.failRequest("test_bar_y_foo_x")

        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.getStatus("test_bar_y_foo_x").apply {
            assertEquals(BatchStatus.StatusFlag.Pending, status)
        }
    }

    @Test
    fun findRequest_notSubmitted_returnsNull() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.findRequest("test_bar_w_foo_z").let(::assertNull)
    }

    @Test
    fun findRequest_submitted_returnsRequest() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.findRequest("test_bar_y_foo_x").let { result ->
            kotlin.test.assertNotNull(result) { br ->
                verify(br) {
                    key("test_bar_y_foo_x")
                    requestUri("test")
                    params("bar=y&foo=x")
                    time(60000)
                }
            }
        }
    }

    @Test
    fun findRequest_multipleSubmitted_returnsLatest() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.failRequest("test_bar_y_foo_x")

        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.findRequest("test_bar_y_foo_x").let { result ->
            kotlin.test.assertNotNull(result) { br ->
                verify(br) {
                    key("test_bar_y_foo_x")
                    requestUri("test")
                    params("bar=y&foo=x")
                    time(60000)
                }
            }
        }
    }

    @Test
    fun nextRequest_emptyQueue_returnsNull() {
        manager.nextRequest().let(::assertNull)
    }

    @Test
    fun nextRequest_allComplete_ReturnsNull() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.finishRequest("test_bar_y_foo_x")

        manager.nextRequest().let(::assertNull)
    }

    @Test
    fun nextRequest_multipleEntries_ReturnsOldestIncomplete() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.submit(BatchRequest("test", "foo=z&bar=w"))
        manager.submit(BatchRequest("test", "foo=a&bar=b"))
        manager.submit(BatchRequest("test", "foo=c&bar=d"))

        manager.finishRequest("test_bar_y_foo_x")
        manager.failRequest("test_bar_w_foo_z")

        manager.nextRequest().let { result ->
            kotlin.test.assertNotNull(result) { br ->
                verify(br) {
                    key("test_bar_b_foo_a")
                    requestUri("test")
                    params("bar=b&foo=a")
                    time(60000)
                }
            }
        }
    }

    @Test
    fun nextRequest_updatesStatusToInProgress() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.nextRequest()

        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("InProgress")
                time(60000)
            }
        }
    }

    @Test
    fun finishRequest_updatesStatusToComplete() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.finishRequest("test_bar_y_foo_x")

        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Completed")
                time(60000)
            }
        }
    }

    @Test
    fun abortRequest_updatesStatusToPending() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.nextRequest()
        manager.abortRequest("test_bar_y_foo_x")

        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Pending")
                time(60000)
            }
        }
    }

    @Test
    fun failRequest_updatesStatusToFailed() {
        manager.submit(BatchRequest("test", "foo=x&bar=y"))
        manager.failRequest("test_bar_y_foo_x")

        jdbc.queryForObject("SELECT * FROM QueueEntry") { rs, _ ->
            verify(rs) {
                index(1)
                key("test_bar_y_foo_x")
                params("bar=y&foo=x")
                requestUri("test")
                status("Failed")
                time(60000)
            }
        }
    }
}