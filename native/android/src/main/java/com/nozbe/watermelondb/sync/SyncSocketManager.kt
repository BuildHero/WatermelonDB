package com.nozbe.watermelondb.sync

object SyncSocketManager {
  @JvmStatic
  fun initialize(socketUrl: String) {
    SyncSocketClient.initialize(socketUrl)
    SyncSocketClient.setStatusListener { status, data ->
      val message = data?.firstOrNull()?.toString()
      nativeOnStatus(status.ordinal, message)
    }
    SyncSocketClient.setCdcListener {
      nativeOnCdc()
    }
    SyncSocketClient.connect()
  }

  @JvmStatic
  fun authenticate(token: String) {
    SyncSocketClient.authenticate(token)
  }

  @JvmStatic
  fun disconnect() {
    SyncSocketClient.disconnect()
  }

  @JvmStatic
  private external fun nativeOnStatus(status: Int, errorMessage: String?)

  @JvmStatic
  private external fun nativeOnCdc()
}
