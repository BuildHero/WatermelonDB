package com.nozbe.watermelondb.slice

import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

object SliceDownloadManager {
    private const val CHUNK_SIZE = 256 * 1024

    private val client: OkHttpClient = OkHttpClient.Builder().build()
    private val calls: ConcurrentHashMap<Long, Call> = ConcurrentHashMap()

    @JvmStatic
    fun startDownload(url: String, handle: Long) {
        val request = Request.Builder().url(url).build()
        val call = client.newCall(request)
        calls[handle] = call

        call.enqueue(object : okhttp3.Callback {
            override fun onFailure(call: Call, e: IOException) {
                calls.remove(handle)
                nativeOnComplete(handle, e.message ?: "Download failed")
            }

            override fun onResponse(call: Call, response: Response) {
                response.use { resp ->
                    if (!resp.isSuccessful) {
                        calls.remove(handle)
                        nativeOnComplete(handle, "HTTP ${resp.code}")
                        return
                    }

                    val body = resp.body
                    if (body == null) {
                        calls.remove(handle)
                        nativeOnComplete(handle, "Empty response body")
                        return
                    }

                    val buffer = ByteArray(CHUNK_SIZE)
                    try {
                        val stream = body.byteStream()
                        while (true) {
                            if (call.isCanceled()) {
                                calls.remove(handle)
                                nativeOnComplete(handle, "Cancelled")
                                return
                            }

                            val read = stream.read(buffer)
                            if (read == -1) {
                                break
                            }
                            nativeOnData(handle, buffer, read)
                        }
                        calls.remove(handle)
                        nativeOnComplete(handle, "")
                    } catch (e: IOException) {
                        calls.remove(handle)
                        nativeOnComplete(handle, e.message ?: "Read failed")
                    }
                }
            }
        })
    }

    @JvmStatic
    fun cancelDownload(handle: Long) {
        val call = calls.remove(handle)
        call?.cancel()
    }

    @JvmStatic
    private external fun nativeOnData(handle: Long, data: ByteArray, length: Int)

    @JvmStatic
    private external fun nativeOnComplete(handle: Long, errorMessage: String)
}
