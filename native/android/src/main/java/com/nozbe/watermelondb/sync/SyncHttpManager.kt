package com.nozbe.watermelondb.sync

import okhttp3.Call
import okhttp3.CookieJar
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

object SyncHttpManager {
    private val client: OkHttpClient = OkHttpClient.Builder()
        .cookieJar(CookieJar.NO_COOKIES)
        .build()
    private val calls: ConcurrentHashMap<Long, Call> = ConcurrentHashMap()

    @JvmStatic
    fun startRequest(
        url: String,
        method: String,
        headerKeys: Array<String>,
        headerValues: Array<String>,
        body: ByteArray?,
        timeoutMs: Int,
        handle: Long
    ) {
        val builder = client.newBuilder()
            .cookieJar(CookieJar.NO_COOKIES)
            .connectTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
            .readTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
            .writeTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)

        val requestBuilder = Request.Builder().url(url)
        for (i in headerKeys.indices) {
            if (i < headerValues.size) {
                requestBuilder.addHeader(headerKeys[i], headerValues[i])
            }
        }

        val requestBody = if (body != null) {
            RequestBody.create("application/json".toMediaTypeOrNull(), body)
        } else null

        requestBuilder.method(method, requestBody)

        val call = builder.build().newCall(requestBuilder.build())
        calls[handle] = call

        call.enqueue(object : okhttp3.Callback {
            override fun onFailure(call: Call, e: IOException) {
                calls.remove(handle)
                nativeOnComplete(handle, 0, null, e.message ?: "Request failed")
            }

            override fun onResponse(call: Call, response: okhttp3.Response) {
                response.use { resp ->
                    calls.remove(handle)
                    val bodyStr = resp.body?.string()
                    nativeOnComplete(handle, resp.code, bodyStr, "")
                }
            }
        })
    }

    @JvmStatic
    fun cancelRequest(handle: Long) {
        val call = calls.remove(handle)
        call?.cancel()
    }

    @JvmStatic
    private external fun nativeOnComplete(handle: Long, statusCode: Int, body: String?, errorMessage: String)
}
