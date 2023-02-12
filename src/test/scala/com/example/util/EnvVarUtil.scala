package com.example.util

import java.util

object EnvVarUtil {

  // generates a warning:
  // WARNING: Illegal reflective access by com.example.ksqldb.EnvVarUtil$ (file:/home/ksilin/code/workspaces/confluent/KSQL/ksqldb_client/target/scala-2.13/test-classes/) to field java.util.Collections$UnmodifiableMap.m
  // looks like the only way around it is to use --add-opens
  def setEnv(key: String, value: String): Unit =
    try {
      val env: util.Map[String, String] = System.getenv
      val cl    = env.getClass
      val field = cl.getDeclaredField("m")
      field.setAccessible(true)
      val writableEnv = field.get(env).asInstanceOf[java.util.Map[String, String]]
      writableEnv.put(key, value)
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to set environment variable", e)
    }

}
