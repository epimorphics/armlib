package com.epimorphics.armlib.impl

import com.epimorphics.armlib.BatchRequest
import com.epimorphics.armlib.BatchStatus
import com.epimorphics.armlib.QueueManager
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.ContextStartedEvent
import org.springframework.context.event.EventListener
import org.springframework.core.annotation.Order
import org.springframework.dao.IncorrectResultSizeDataAccessException
import org.springframework.jdbc.core.ConnectionCallback
import org.springframework.jdbc.core.JdbcOperations
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet
import javax.ws.rs.core.MultivaluedMap

class SqlQueueManager(
        private val jdbc: JdbcOperations,
        private val config: Config = Config(false, 500)
): QueueManager {

    private val incompleteStatus get() = arrayOf(
            BatchStatus.StatusFlag.Pending.name,
            BatchStatus.StatusFlag.InProgress.name
    )

    @Order(0)
    @EventListener(ContextRefreshedEvent::class)
    fun onStart() {
        jdbc.execute(ConnectionCallback { con ->
            con.metaData.getTables(null, null, "queue", arrayOf("TABLE")).apply {
                if (!next()) {
                    val sql = StringBuilder()
                            .appendln("CREATE TABLE queue (")
                            .appendln("index serial PRIMARY KEY,")
                            .appendln("key varchar(255) NOT NULL,")
                            .appendln("status varchar(50) NOT NULL,")
                            .appendln("requestUri varchar(255) NOT NULL,")
                            .appendln("params varchar,")
                            .appendln("estimatedTime bigint,")
                            .appendln("startTime bigint")
                            .appendln(");")
                            .appendln("CREATE INDEX ON queue (key) ;")
                            .appendln("CREATE INDEX ON queue (status) ;")
                            .toString()
                    con.createStatement().execute(sql)
                }
            }.close()
        })
    }

    override fun submit(request: BatchRequest): BatchStatus {
        val key = request.key
        val qe = getQueueEntry(key)
                ?.takeUnless(QueueEntry::isFailed)
                ?: newQueueEntry(request)

        return qe.asBatchStatus()
    }

    override fun resubmit(request: BatchRequest): BatchStatus {
        val key = request.key
        val status = BatchStatus.StatusFlag.Pending
        val requestUri = request.requestURI

        val sql = StringBuilder()
                .appendln("DELETE FROM queue WHERE key = ? AND status = ANY(?) ;")
                .appendln("INSERT INTO queue (key, status, requestUri, params, estimatedTime) VALUES (?, ?, ?, ?, ?) ;")
                .toString()

        jdbc.update(sql, key, incompleteStatus, key, status.name, requestUri, request.parameterString, request.estimatedTime)

        return QueueEntry(key, status, requestUri, request.parameters, request.estimatedTime).asBatchStatus()
    }

    override fun getStatus(requestKey: String): BatchStatus {
        return getQueueEntry(requestKey)
                ?.asBatchStatus()
                ?: BatchStatus(requestKey, BatchStatus.StatusFlag.Unknown)
    }

    override fun getQueue(): MutableList<BatchStatus> {
        val sql = StringBuilder()
                .appendln("SELECT *")
                .appendln("FROM queue")
                .appendln("WHERE status = ANY(?)")
                .appendln("ORDER BY index ASC ;")
                .toString()

        return jdbc.query<QueueEntry>(sql, rowMapper, incompleteStatus)
                .map(QueueEntry::asBatchStatus)
                .toMutableList()
    }

    override fun findRequest(key: String): BatchRequest? {
        return getQueueEntry(key)?.asBatchRequest()
    }

    override fun nextRequest(timeout: Long): BatchRequest? {
        val sql = StringBuilder()
                .appendln("SELECT *")
                .appendln("FROM queue")
                .appendln("WHERE status = ?")
                .appendln("ORDER BY index ASC")
                .appendln("LIMIT 1 ;")
                .toString()

        return try {
            jdbc.queryForObject<QueueEntry>(sql, rowMapper, BatchStatus.StatusFlag.Pending.name)
                    ?.asBatchRequest()
                    ?.apply {
                        startQueueEntry(key)
                    }
        } catch (e: IncorrectResultSizeDataAccessException) {
            Thread.sleep(timeout)
            null
        }
    }

    private fun startQueueEntry(key: String) {
        val now = System.currentTimeMillis()
        val sql = StringBuilder()
                .appendln("UPDATE queue")
                .appendln("SET status = ?, startTime = ?")
                .appendln("WHERE key = ?")
                .appendln("AND status = ? ;")
                .toString()

        jdbc.update(sql, BatchStatus.StatusFlag.InProgress.name, now, key, BatchStatus.StatusFlag.Pending.name)
    }

    override fun nextRequest(): BatchRequest? {
        return nextRequest(1000)
    }

    override fun finishRequest(key: String) {
        if (config.deleteOnComplete) {
            deleteQueueEntry(key)
        } else {
            updateQueueEntry(key, BatchStatus.StatusFlag.Completed)
        }
    }

    override fun abortRequest(key: String) {
        updateQueueEntry(key, BatchStatus.StatusFlag.Pending)
    }

    override fun failRequest(key: String) {
        updateQueueEntry(key, BatchStatus.StatusFlag.Failed)
    }

    override fun removeOldCompletedRequests(cutoff: Long) {
        TODO("not implemented")
    }

    private fun getQueueEntry(key: String): QueueEntry? {
        val sql = StringBuilder()
                .appendln("SELECT * FROM queue")
                .appendln("WHERE key = ?")
                .appendln("ORDER BY index DESC")
                .appendln("LIMIT 1 ;")
                .toString()

        return try {
            jdbc.queryForObject<QueueEntry>(sql, rowMapper, key)
        } catch (e: IncorrectResultSizeDataAccessException) {
            null
        }
    }

    private fun newQueueEntry(request: BatchRequest): QueueEntry {
        val key = request.key
        val status = BatchStatus.StatusFlag.Pending
        val requestUri = request.requestURI

        jdbc.update(
                "INSERT INTO queue (key, status, requestUri, params, estimatedTime) VALUES (?, ?, ?, ?, ?) ;",
                request.key,
                status.name,
                request.requestURI,
                request.parameterString,
                request.estimatedTime
        )

        return QueueEntry(key, status, requestUri, request.parameters, request.estimatedTime)
    }

    private fun updateQueueEntry(key: String, status: BatchStatus.StatusFlag) {
        val sql = StringBuilder()
                .appendln("UPDATE queue")
                .appendln("SET status = ?")
                .appendln("WHERE key = ?")
                .appendln("AND status = ANY(?) ;")
                .toString()

        jdbc.update(sql, status.name, key, incompleteStatus)
    }

    private fun deleteQueueEntry(key: String) {
        jdbc.update("DELETE FROM queue WHERE key = ? AND status = ANY(?);", key, incompleteStatus)
    }

    private val rowMapper: RowMapper<QueueEntry> = RowMapper { rs: ResultSet, _: Int ->
        val key = rs.getString("key")
        val status = rs.getString("status").let(BatchStatus.StatusFlag::valueOf)
        val requestUri = rs.getString("requestUri")
        val params = rs.getString("params").takeUnless(String::isEmpty)?.let(BatchRequest::decodeParameterString) ?: MultivaluedStringMap()
        val estimatedTime = rs.getLong("estimatedTime")
        val startTime = rs.getLong("startTime")

        QueueEntry(key, status, requestUri, params, estimatedTime, startTime)
    }

    private class QueueEntry(
            private val key: String,
            private val status: BatchStatus.StatusFlag,
            private val requestUri: String,
            private val params: MultivaluedMap<String, String>,
            private val estimatedTime: Long?,
            private val startTime: Long? = null
    ) {
        fun asBatchStatus(): BatchStatus {
            return BatchStatus(key, status).also { bs ->
                estimatedTime?.let(bs::setEstimatedTime)
                startTime?.let(bs::setStarted)
            }
        }

        fun asBatchRequest(): BatchRequest {
            return BatchRequest(requestUri, params).also { br ->
                estimatedTime?.let(br::setEstimatedTime)
            }
        }

        fun isFailed(): Boolean {
            return status == BatchStatus.StatusFlag.Failed
        }
    }

    class Config(
            val deleteOnComplete: Boolean,
            val queryInterval: Int
    )
}