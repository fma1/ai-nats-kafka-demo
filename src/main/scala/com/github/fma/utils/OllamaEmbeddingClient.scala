package com.github.fma.utils

class OllamaEmbeddingClient(host: String, port: Int) {

  private val baseUrl = s"http://$host:$port"

  def embed(text: String): List[Double] = {

    val payload =
      s"""{"model":"nomic-embed-text","prompt":${escape(text)}}"""

    val response = requests.post(
      url = baseUrl + "/api/embeddings",
      data = payload,
      headers = Seq("Content-Type" -> "application/json")
    )

    ujson.read(response.text())("embedding").arr.map(_.num).toList
  }

  private def escape(s: String): String =
    "\"" + s.replace("\"", "\\\"") + "\""
}