"""Credential selection must work without putting gateway secrets in YAML."""

from importlib import import_module
from pathlib import Path
import os
import subprocess
import sys

import httpx
import pytest

from sovereign_hall.core.config import Config
from sovereign_hall.services.experiment_runtime import snapshot_code
from sovereign_hall.services.llm_client import LLMClient


@pytest.mark.parametrize("environment,file_value,explicit,expected", [
    ("environment-key", "file-key", "explicit-key", "explicit-key"),
    ("environment-key", "file-key", None, "environment-key"),
    (None, "file-key", None, "file-key"),
    (None, None, None, "inline-key"),
])
def test_client_credential_precedence(
    tmp_path, monkeypatch, environment, file_value, explicit, expected,
):
    name = "SOVEREIGN_TEST_API_KEY"
    config = object.__new__(Config)
    config._config = {"llm": {"api_key_env": name, "api_key": "inline-key"}}
    env_file = tmp_path / ".env"
    env_file.write_text(f"{name}={file_value}\n" if file_value else "")
    if environment:
        monkeypatch.setenv(name, environment)
    else:
        monkeypatch.delenv(name, raising=False)
    config_module = import_module("sovereign_hall.core.config")
    client_module = import_module("sovereign_hall.services.llm_client")
    monkeypatch.setattr(config_module, "find_dotenv", lambda: str(env_file))
    monkeypatch.setattr(client_module, "get_config", lambda: config)
    monkeypatch.setattr(LLMClient, "_init_client", lambda self: None)

    client = LLMClient(api_key=explicit)
    assert client.api_key == expected
    assert "inline-key" not in str(config.to_dict())


def test_disabled_environment_credentials_do_not_read_dotenv(monkeypatch):
    config = object.__new__(Config)
    config._config = {"security": {"use_env_vars": False}}
    monkeypatch.setenv("SOVEREIGN_TEST_API_KEY", "environment-key")
    module = import_module("sovereign_hall.core.config")

    def unexpected_read():
        raise AssertionError("disabled credentials must not read .env")

    monkeypatch.setattr(module, "find_dotenv", unexpected_read)
    assert config.get_api_key("SOVEREIGN_TEST_API_KEY") is None


def test_new_snapshot_can_resolve_project_credentials_without_copying_them(tmp_path):
    root = Path(__file__).resolve().parents[1]
    runtime = tmp_path / "runs" / "runtime" / "sovereign_hall"
    manifest = snapshot_code(root, runtime)
    (tmp_path / ".env").write_text("SOVEREIGN_TEST_API_KEY=snapshot-fixture\n")
    assert ".env" not in manifest
    assert not (runtime / ".env").exists()
    env = {key: value for key, value in os.environ.items()
           if key != "SOVEREIGN_TEST_API_KEY"}
    env["PYTHONPATH"] = str(runtime.parent)
    result = subprocess.run(
        [sys.executable, "-B", "-c", (
            "from sovereign_hall.core.config import get_config; "
            "assert get_config().get_api_key('SOVEREIGN_TEST_API_KEY') "
            "== 'snapshot-fixture'"
        )],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_named_models_keep_independent_endpoints_and_credentials(monkeypatch):
    config = object.__new__(Config)
    config.load_defaults()
    monkeypatch.setenv("OPENAI_API_KEY", "gateway-fixture")
    module = import_module("sovereign_hall.services.llm_client")
    monkeypatch.setattr(module, "get_config", lambda: config)
    monkeypatch.setattr(LLMClient, "_init_client", lambda self: None)
    before = config.to_dict()
    clients = [LLMClient(), LLMClient(profile="kimi-k3"),
               LLMClient(profile="glm-5.2-fp8")]
    expected = [
        ("kimi-k3", "https://api.nextoken.real-ai.cn/v1", "gateway-fixture"),
        ("kimi-k3", "https://api.nextoken.real-ai.cn/v1", "gateway-fixture"),
        ("GLM-5.2-FP8", "http://172.18.5.19:8000/v1", "not_need"),
    ]
    observed = []

    def handler(request):
        import json
        observed.append((json.loads(request.content)["model"],
                         str(request.url), request.headers["Authorization"]))
        assert request.headers["Host"] == request.url.netloc.decode()
        return httpx.Response(200, json={"choices": [{
            "message": {"content": '{"status":"ok"}'}, "finish_reason": "stop",
        }]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        for client, (model, base_url, key) in zip(clients, expected):
            assert (client.model, client.base_url, client.api_key) == (model, base_url, key)
            client._http_client = transport
            result, _ = await client._openai_chat([], 0.7, 100, json_output=True)
            assert result == '{"status":"ok"}'

    assert observed == [(model, base_url + "/chat/completions", "Bearer " + key)
                        for model, base_url, key in expected]
    assert config.to_dict() == before
    with pytest.raises(ValueError, match="Unknown LLM profile"):
        LLMClient(profile="missing-deployment")
