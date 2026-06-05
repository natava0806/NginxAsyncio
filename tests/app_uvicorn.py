async def app(scope, receive, send):
    if scope["type"] != "http": return
    body = b""
    more_body = True
    while more_body:
        message = await receive()
        body += message.get("body", b"")
        more_body = message.get("more_body", False)
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            (b"content-type", b"text/plain; charset=utf-8"),
            (b"connection", b"close")
        ]
    })
    await send({
        "type": "http.response.body",
        "body": body if body else b"Hello from ASGI Uvicorn!"
    })

