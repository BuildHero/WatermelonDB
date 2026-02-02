package com.nozbe.watermelondb.sync

import io.socket.client.IO
import io.socket.client.Socket
import io.socket.emitter.Emitter
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Interceptor
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import okhttp3.MediaType.Companion.toMediaType
import org.json.JSONObject
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import android.util.Log

object SyncSocketClient {
  private var socket: Socket? = null
  private var statusListener: ((SyncSocketStatus, Array<Any>?) -> Unit)? = null
  private var cdcListener: (() -> Unit)? = null

  private const val TAG = "SyncSocketClient"

  enum class SyncSocketStatus {
    CONNECTED, DISCONNECTED, ERROR
  }

  fun initialize(socketUrl: String) {
    socket?.let { existing ->
      existing.off()
      existing.disconnect()
    }
    socket = null
    val options = IO.Options()

    val safeInterceptor = Interceptor { chain ->
      try {
        chain.proceed(chain.request())
      } catch (e: Exception) {
        Log.e(TAG, "Exception in OkHttp intercepted: ${e.message}")
        statusListener?.invoke(SyncSocketStatus.ERROR, arrayOf(e))

        val emptyJsonBody = "{}".toResponseBody("application/json".toMediaType())

        Response.Builder()
          .code(200)
          .message("OK")
          .body(emptyJsonBody)
          .request(chain.request())
          .protocol(okhttp3.Protocol.HTTP_1_1)
          .build()
      }
    }

    val okHttpClient: OkHttpClient = OkHttpClient.Builder()
      .cookieJar(object : CookieJar {
        private val cookieStore = ConcurrentHashMap<String, MutableSet<Cookie>>()

        override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
          val domain = url.host
          cookieStore.compute(domain) { _, currentCookies ->
            val newCookies = currentCookies?.toMutableSet() ?: CopyOnWriteArraySet<Cookie>()
            newCookies.addAll(cookies)
            newCookies
          }
        }

        override fun loadForRequest(url: HttpUrl): List<Cookie> {
          return cookieStore[url.host]?.toList() ?: emptyList()
        }
      })
      .readTimeout(1, TimeUnit.MINUTES)
      .addInterceptor(safeInterceptor)
      .build()

    options.callFactory = okHttpClient
    options.webSocketFactory = okHttpClient

    socket = IO.socket(socketUrl, options)
  }

  fun connect() {
    registerListeners()
    socket?.connect()
  }

  fun disconnect() {
    socket?.disconnect()
    removeListeners()
  }

  fun authenticate(token: String) {
    val payload = JSONObject()
    payload.put("token", token)
    socket?.emit("sync::auth", payload)
  }

  fun setStatusListener(listener: ((SyncSocketStatus, Array<Any>?) -> Unit)?) {
    statusListener = listener
  }

  fun setCdcListener(listener: (() -> Unit)?) {
    cdcListener = listener
  }

  private fun registerListeners() {
    socket?.apply {
      on("connect", onClientConnected)
      on("disconnect", onClientDisconnected)
      on("error", onClientError)
      on("cdc", onCDCEvent)
    }
  }

  private fun removeListeners() {
    socket?.off()
  }

  private val onClientConnected = Emitter.Listener {
    statusListener?.invoke(SyncSocketStatus.CONNECTED, null)
  }

  private val onClientDisconnected = Emitter.Listener {
    statusListener?.invoke(SyncSocketStatus.DISCONNECTED, null)
  }

  private val onClientError = Emitter.Listener {
    statusListener?.invoke(SyncSocketStatus.ERROR, it)
  }

  private val onCDCEvent = Emitter.Listener {
    cdcListener?.invoke()
  }
}
