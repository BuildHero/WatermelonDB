package com.nozbe.watermelondb

import android.util.Log
import com.github.luben.zstd.ZstdInputStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException

object ZstdFileUtil {
    private const val TAG = "ZstdFileUtil"
    private const val BUFFER_SIZE = 1024 * 1024 // 1MB buffer

    @JvmStatic
    @Throws(Exception::class)
    fun decompressZstd(src: String, dest: String) {
        Log.i(TAG, "Starting zstd decompression from $src to $dest")

        val srcFile = File(src)
        val destFile = File(dest)

        if (!srcFile.exists()) {
            throw IOException("Source file not found: $src")
        }

        try {
            if (destFile.exists()) {
                destFile.delete()
            }
            destFile.parentFile?.mkdirs()

            BufferedInputStream(FileInputStream(srcFile), BUFFER_SIZE).use { fileInput ->
                ZstdInputStream(fileInput).use { zstdInput ->
                    BufferedOutputStream(FileOutputStream(destFile), BUFFER_SIZE).use { fileOutput ->
                        val buffer = ByteArray(BUFFER_SIZE)
                        var bytesRead: Int
                        while (zstdInput.read(buffer).also { bytesRead = it } != -1) {
                            fileOutput.write(buffer, 0, bytesRead)
                        }
                        fileOutput.flush()
                    }
                }
            }

            Log.i(TAG, "Decompression completed successfully")
        } catch (e: Exception) {
            Log.e(TAG, "Decompression error: ${e.message}", e)
            if (destFile.exists()) {
                destFile.delete()
            }
            throw e
        }
    }
}
