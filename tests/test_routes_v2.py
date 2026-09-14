import io

from cinc.app import create_app


def test_text_upload_route_persists_and_reopens(tmp_path):
    app = create_app(tmp_path)
    client = app.test_client()
    response = client.post(
        "/logs/import",
        data={
            "file": (
                io.BytesIO(b"2026-01-01T00:00:00Z ERROR api failed"),
                "application.log",
            )
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 201
    body = response.get_json()
    assert body["recordId"]
    assert client.get(body["viewUrl"]).status_code == 200
    assert app.extensions["cinc"]["store"].count("text_log") == 1


def test_import_route_returns_actionable_json_error(tmp_path):
    client = create_app(tmp_path).test_client()
    response = client.post("/logs/import", json={"unknown": True})
    assert response.status_code == 400
    assert response.get_json()["error"]
