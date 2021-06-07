import os


pytest_plugins = (
    ["tests.integration.conftest_ci"]
    if os.environ.get("env") == "CI"
    else ["tests.integration.conftest_local", "docker_compose"]
)
