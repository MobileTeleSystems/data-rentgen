<a id="http2kafka-openapi"></a>

# OpenAPI specification

<!-- this page cannot be properly rendered in local environment, it should be build in CI first --><!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta
    name="description"
    content="DataRentgen HTTP2Kafka - SwaggerUI"
  />
  <title>DataRentgen HTTP2Kafka - SwaggerUI</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css" />
  <link rel="shortcut icon" href="../_static/icon.svg">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({
        url: '../../_static/openapi_http2kafka.json',
        dom_id: '#swagger-ui',
      });
    };
  </script>
</body>
</html>
